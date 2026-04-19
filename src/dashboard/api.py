import json
import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Initialize FastAPI App
app = FastAPI(
    title="Amazon Intelligence - Recommendation API",
    description="Real-time Amazon product recommendations powered by Apache Spark ALS.",
    version="1.0.0"
)

# Configure CORS (Allow all origins as requested)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection parameters
DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow_pass"
}

def get_db_connection():
    """Establish and return a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@app.get("/api/recommend/{user_id}")
def get_recommendations(user_id: str):
    """
    Fetch personalized recommendations for a specific user ID from PostgreSQL.
    Returns a JSON array of recommended products.
    """
    conn = None
    try:
        conn = get_db_connection()
        # Using RealDictCursor for cleaner data access
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Spark usually preserves case when creating tables, so we quote "UserId"
        query = 'SELECT recommendations FROM user_recommendations WHERE "UserId" = %s LIMIT 1;'
        cur.execute(query, (user_id,))
        row = cur.fetchone()
        
        if not row:
            raise HTTPException(
                status_code=404, 
                detail=f"Aucune recommandation trouvée pour l'utilisateur '{user_id}'. Il est soit inconnu, soit en attente de traitement."
            )
            
        recs_data = row['recommendations']
        
        # Spark's to_json() function saves the array as a JSON string in the DB.
        if isinstance(recs_data, str):
            try:
                recs_data = json.loads(recs_data)
            except json.JSONDecodeError:
                raise HTTPException(status_code=500, detail="Invalid JSON format in database.")

                return {
            "status": "success",
            "user_id": user_id,
            "data": recs_data
        }
        
    

    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")
    finally:
        if conn:
            cur.close()
            conn.close()
