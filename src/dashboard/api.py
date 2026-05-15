import os
import json
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

# Database connection parameters via environment variables
DB_PARAMS = {
    "host": os.getenv("API_DB_HOST", "postgres"),
    "port": int(os.getenv("API_DB_PORT", 5432)),
    "dbname": os.getenv("POSTGRES_DB", "airflow"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow_pass")
}

# 3. Initialize Connection Pool
try:
    db_pool = SimpleConnectionPool(1, 20, **DB_PARAMS)
except psycopg2.Error as e:
    print(f"Error initializing connection pool: {e}")
    db_pool = None

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

@app.get("/")
def serve_dashboard():
    """
    Sert le fichier statique index.html qui sert de dashboard (interface web).
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    index_path = os.path.join(base_dir, "index.html")
    
    if os.path.exists(index_path):
        return FileResponse(index_path)
    else:
        raise HTTPException(status_code=404, detail="Le fichier index.html du dashboard est introuvable.")

# Produits populaires de secours pour le Cold Start
POPULAR_PRODUCTS = ["B007JFMCB6", "B000HDOPZG", "B002QWP8H0", "B000KV61FC", "B000N9912G"]

@app.get("/recommendations/user/{user_id}")
def get_recommendations(user_id: str):
    """
    Fetch personalized recommendations for a specific user ID from PostgreSQL.
    If user is unknown, returns a fallback list of popular products (Cold Start).
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool is not available.")

    conn = None
    cur = None
    try:
        # Get connection from pool
        conn = db_pool.getconn()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = 'SELECT recommendations FROM user_recommendations WHERE "UserId" = %s LIMIT 1;'
        cur.execute(query, (user_id,))
        row = cur.fetchone()
        
        # 1. Gestion du Cold Start : renvoyer les produits populaires par défaut
        if not row:
            return {
                "user_id": user_id,
                "recommendations": POPULAR_PRODUCTS
            }
            
        recs_data = row['recommendations']
        
        # Spark's to_json() function saves the array as a JSON string in the DB.
        if isinstance(recs_data, str):
            try:
                recs_data = json.loads(recs_data)
            except json.JSONDecodeError:
                recs_data = []
                
        # Extraction des identifiants de produits sous forme de simple liste de chaînes
        formatted_recs = []
        for item in recs_data:
            if isinstance(item, dict):
                # ALS retourne des structures de type {"item_index": 12, "rating": 4.5}
                if "item_index" in item:
                    formatted_recs.append(str(item["item_index"]))
                elif "ProductId" in item:
                    formatted_recs.append(str(item["ProductId"]))
                else:
                    formatted_recs.append(str(item))
            else:
                formatted_recs.append(str(item))
                
        # 2. Respect strict du format JSON de réponse attendu
        return {
            "user_id": user_id,
            "recommendations": formatted_recs
        }
        
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")
    finally:
        if cur:
            cur.close()
        if conn:
            # Release connection back to the pool
            db_pool.putconn(conn)

@app.get("/api/model/metrics")
def get_model_metrics():
    """
    Récupère les métriques du modèle MLOps (RMSE, Rank, RegParam) à partir de metrics.json.
    """
    metrics_path = "/opt/spark/models/als_model/metrics.json"
    if os.path.exists(metrics_path):
        with open(metrics_path, "r") as f:
            return json.load(f)
    return {"rmse": "N/A", "rank": "N/A", "regParam": "N/A", "error": "Fichier introuvable"}

@app.get("/api/model/versions")
def get_model_versions():
    """
    Scanne le répertoire d'archives pour renvoyer la liste des modèles enregistrés.
    """
    archive_dir = "/opt/spark/models/archive/"
    versions = []
    if os.path.exists(archive_dir):
        for item in os.listdir(archive_dir):
            if os.path.isdir(os.path.join(archive_dir, item)) and item.startswith("als_model_"):
                versions.append(item.replace("als_model_", ""))
    return sorted(versions, reverse=True)

@app.get("/api/streaming/stats")
def get_streaming_stats():
    """
    Récupère le nombre total de lignes dans la base de données en temps réel pour le Dashboard MLOps.
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database pool not available.")
    
    conn = None
    cur = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM user_recommendations;")
        count = cur.fetchone()[0]
        return {"total_recommendations": count}
    except Exception as e:
        return {"total_recommendations": "Erreur SQL", "details": str(e)}
    finally:
        if cur:
            cur.close()
        if conn:
            db_pool.putconn(conn)

