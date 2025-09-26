#!/usr/bin/env python3
"""
Test de la configuration
"""

from etl_package.config.config import config

def test_config():
    print("=== TEST CONFIGURATION ===")
    print(f"DB_HOST: {config.DB_HOST}")
    print(f"DB_PORT: {config.DB_PORT}") 
    print(f"DB_NAME: {config.DB_NAME}")
    print(f"DB_USER: {config.DB_USER}")
    print(f"LOG_LEVEL: {config.LOG_LEVEL}")
    print(f"URL DB: {config.database_url.replace(config.DB_PASSWORD, '***')}")
    
    # Tester l'import des autres modules
    try:
        from etl_package.extract.web_scraper import scrape_cacao_ratings
        print("✅ Module web_scraper importé")
    except ImportError as e:
        print(f"❌ Erreur import web_scraper: {e}")

if __name__ == "__main__":
    test_config()