#!/usr/bin/env python3
"""
Test complet du pipeline ETL
"""

import logging
from etl_package.extract.web_scraper import scrape_cacao_ratings
from etl_package.transform.missing_values import MissingValuesHandler
from etl_package.load.to_postgres import PostgreSQLLoader

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_pipeline_etape_par_etape():
    """Test du pipeline étape par étape"""
    print("=== TEST PIPELINE ÉTAPE PAR ÉTAPE ===")
    
    try:
        # 1. EXTRACTION
        print("1. 📥 EXTRACTION...")
        df = scrape_cacao_ratings()
        if df is None or df.empty:
            print("❌ Échec de l'extraction")
            return False
        print(f"   ✅ {df.shape[0]} lignes extraites")
        
        # 2. TRANSFORMATION
        print("2. 🔄 TRANSFORMATION...")
        df = MissingValuesHandler.impute_missing_values(df)
        print("   ✅ Données transformées")
        
        # 3. CHARGEMENT
        print("3. 📤 CHARGEMENT...")
        loader = PostgreSQLLoader()
        success = loader.load_dataframe(df, 'cacao_ratings_test')
        
        if success:
            print("🎉 Pipeline terminé avec succès!")
            return True
        else:
            print("❌ Échec du chargement")
            return False
            
    except Exception as e:
        print(f"💥 Erreur: {e}")
        return False

if __name__ == "__main__":
    test_pipeline_etape_par_etape()