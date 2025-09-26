#!/usr/bin/env python3
"""
Test de la connexion à la base de données PostgreSQL
"""

from etl_package.config import config
from etl_package.load.to_postgres import PostgreSQLLoader

def test_database_connection():
    print("=== TEST CONNEXION POSTGRESQL ===")
    
    # Afficher la configuration
    print(f"📋 Configuration:")
    print(f"   Host: {config.DB_HOST}")
    print(f"   Port: {config.DB_PORT}")
    print(f"   Database: {config.DB_NAME}")
    print(f"   User: {config.DB_USER}")
    print(f"   URL: {config.database_url}")
    
    # Tester la connexion
    try:
        loader = PostgreSQLLoader()
        print("✅ Connexion PostgreSQL réussie!")
        
        # Tester avec un DataFrame simple
        import pandas as pd
        test_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Test1', 'Test2', 'Test3'],
            'value': [10.5, 20.3, 30.1]
        })
        
        success = loader.load_dataframe(test_data, 'test_table', if_exists='replace')
        if success:
            print("✅ Table test créée avec succès!")
        else:
            print("❌ Erreur lors de la création de la table")
            
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        print("💡 Vérifiez que:")
        print("   - PostgreSQL est démarré")
        print("   - La base de données existe")
        print("   - Les identifiants dans .env sont corrects")

if __name__ == "__main__":
    test_database_connection()