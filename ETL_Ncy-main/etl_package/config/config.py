"""
Module de configuration pour les paramètres de connexion aux bases de données
et autres paramètres globaux de l'application.
"""

import os
from dotenv import load_dotenv
from pathlib import Path

def load_environment():
    """Charge les variables d'environnement avec gestion d'erreurs"""
    try:
        # Essayer plusieurs chemins possibles
        possible_paths = [
            Path(__file__).parent.parent.parent / '.env',  # Racine du projet
            Path(__file__).parent.parent / '.env',         # Dossier etl_package
            Path('.env'),                                  # Dossier courant
            Path('..') / '.env',                           # Dossier parent
        ]
        
        for env_path in possible_paths:
            if env_path.exists():
                load_dotenv(env_path)
                print(f"✅ Fichier .env chargé depuis: {env_path}")
                return True
        
        print("⚠️  Aucun fichier .env trouvé, utilisation des valeurs par défaut")
        return False
        
    except Exception as e:
        print(f"⚠️  Erreur lors du chargement du .env: {e}")
        return False

# Charger l'environnement
load_environment()

class Config:
    """Classe de configuration pour les paramètres de la base de données"""
    
    # Configuration PostgreSQL
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'etl_database')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    
    # Autres configurations
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', '1000'))
    
    @property
    def database_url(self):
        """Retourne l'URL de connexion à la base de données PostgreSQL"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    def __str__(self):
        """Affichage de la configuration (masque le mot de passe)"""
        return (f"Config(DB_HOST={self.DB_HOST}, DB_PORT={self.DB_PORT}, "
                f"DB_NAME={self.DB_NAME}, DB_USER={self.DB_USER}, "
                f"LOG_LEVEL={self.LOG_LEVEL})")

# Instance globale de configuration
config = Config()

# Test de la configuration au chargement
if __name__ == "__main__":
    print("=== TEST CONFIGURATION ===")
    print(config)
    print(f"URL de base de données: {config.database_url.replace(config.DB_PASSWORD, '***')}")