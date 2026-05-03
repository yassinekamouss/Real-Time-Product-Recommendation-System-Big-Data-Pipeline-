import os
import logging
from typing import List
from fastapi import FastAPI, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import json

# Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialisation de l'application FastAPI
app = FastAPI(
    title="Amazon Product Recommendation API",
    description="API REST pour servir les recommandations générées par Apache Spark ALS",
    version="1.0.0"
)

# Configuration CORS (Indispensable pour que le Frontend puisse appeler l'API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # En production, remplacez "*" par l'URL de votre frontend (ex: http://localhost:3000)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Variables d'environnement pour la connexion PostgreSQL
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")

def get_db_connection():
    """Crée et retourne une connexion à la base de données PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion à la base de données : {e}")
        raise HTTPException(status_code=500, detail="Database connection error")

# Modèles Pydantic pour la documentation Swagger
class Recommendation(BaseModel):
    ProductId: str
    score: float

class UserRecommendationResponse(BaseModel):
    user_id: str
    recommendations: List[Recommendation] # Changement de format pour inclure le score

@app.get("/", tags=["Health Check"])
def read_root():
    return {"status": "API is running", "message": "Welcome to the Recommendation API"}

@app.get("/recommendations/user/{user_id}", response_model=UserRecommendationResponse, tags=["Recommendations"])
def get_recommendations_for_user(
    user_id: str = Path(..., title="L'identifiant de l'utilisateur", example="A3SGXH7AUHU8GW")
):
    """
    Récupère le Top-N des recommandations pour un utilisateur spécifique.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Requête pour chercher les recommandations (on cherche la correspondance exacte de l'ID string)
        cur.execute(
            """
            SELECT recommendations 
            FROM user_recommendations 
            WHERE "UserId" = %s
            """,
            (user_id,)
        )
        
        result = cur.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Aucune recommandation trouvée pour l'utilisateur '{user_id}'. Il est peut-être inactif ou nouveau.")

        # Dans PostgreSQL, on a stocké un texte JSON. On doit le parser.
        recommendations_data = json.loads(result['recommendations'])

        return {
            "user_id": user_id,
            "recommendations": recommendations_data
        }

    except psycopg2.Error as e:
        logger.error(f"Erreur PostgreSQL : {e}")
        raise HTTPException(status_code=500, detail="Erreur interne de la base de données")
    finally:
        if conn:
            conn.close()

# Permet de lancer le serveur en développement local
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)