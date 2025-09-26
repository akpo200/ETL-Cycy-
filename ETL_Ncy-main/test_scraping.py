#!/usr/bin/env python3
"""
Test du scraping corrigé
"""

from etl_package.extract.web_scraper import scrape_cacao_ratings

def test_scraping():
    print("=== TEST SCRAPING CORRIGÉ ===")
    
    try:
        df = scrape_cacao_ratings()
        
        if df is not None and not df.empty:
            print(f"✅ Scraping réussi!")
            print(f"📊 Shape: {df.shape}")
            print(f"🏷️  Colonnes: {df.columns.tolist()}")
            print("\n📋 Aperçu des données:")
            print(df.head())
            
            # Sauvegarder pour inspection
            df.to_csv('donnees_scrapees.csv', index=False)
            print("💾 Données sauvegardées dans 'donnees_scrapees.csv'")
        else:
            print("❌ Aucune donnée scrapée")
            
    except Exception as e:
        print(f"💥 Erreur: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_scraping() 