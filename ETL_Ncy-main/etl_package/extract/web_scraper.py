"""
Module d'extraction de données via web scraping avec BeautifulSoup.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)

def scrape_cacao_ratings():
    """
    Scrape les données de ratings de cacao depuis le site Codecademy.
    
    Returns:
        pd.DataFrame: DataFrame contenant les données de ratings de cacao
    """
    try:
        url = "https://content.codecademy.com/courses/beautifulsoup/cacao/index.html"
        logger.info(f"Scraping des données depuis: {url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parsing avec BeautifulSoup
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Récupérer la meilleure table (plus grande en taille)
        df = manual_table_scraping(soup)
        if df is not None and not df.empty:
            logger.info(f"✅ Scraping manuel réussi: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            return df
        
        logger.error("Aucune donnée n'a pu être scrapée")
        return pd.DataFrame()
        
    except requests.RequestException as e:
        logger.error(f"Erreur réseau lors du scraping: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Erreur inattendue lors du scraping: {e}")
        return pd.DataFrame()

def manual_table_scraping(soup):
    """
    Scraping manuel de la table HTML avec BeautifulSoup.
    
    Args:
        soup: Objet BeautifulSoup
        
    Returns:
        pd.DataFrame: DataFrame des données scrapées
    """
    try:
        tables = soup.find_all('table')
        logger.info(f"Nombre de tables trouvées: {len(tables)}")
        
        if not tables:
            logger.warning("Aucune table trouvée dans le HTML")
            return None
        
        candidate_dfs = []  # stocker toutes les tables converties
        
        for i, table in enumerate(tables):
            logger.info(f"Traitement de la table {i+1}")
            
            rows = table.find_all('tr')
            data = []
            for row in rows:
                cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
                if cells:
                    data.append(cells)
            
            logger.info(f"Lignes totales trouvées: {len(data)}")
            if not data or len(data) < 2:
                continue
            
            # Vérifier si la première ligne peut être des en-têtes
            potential_headers = data[0]
            if all(isinstance(h, str) and len(h) > 0 for h in potential_headers):
                headers = potential_headers
                data_rows = data[1:]
            else:
                headers = [f"Colonne_{j+1}" for j in range(len(data[0]))]
                data_rows = data
            
            # Uniformiser les lignes
            max_cols = len(headers)
            normalized_rows = []
            for row in data_rows:
                if len(row) < max_cols:
                    row.extend([''] * (max_cols - len(row)))
                elif len(row) > max_cols:
                    row = row[:max_cols]
                normalized_rows.append(row)
            
            df = pd.DataFrame(normalized_rows, columns=headers)
            logger.info(f"Table {i+1} créée avec shape {df.shape}")
            
            if not df.empty:
                candidate_dfs.append(df)
        
        if candidate_dfs:
            # Choisir la plus grande table (celle avec le plus de lignes * colonnes)
            best_df = max(candidate_dfs, key=lambda t: t.shape[0] * t.shape[1])
            return best_df
        
        logger.warning("Aucune table exploitable trouvée")
        return None
    
    except Exception as e:
        logger.error(f"Erreur lors du scraping manuel: {e}")
        return None

# Test
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
    df = scrape_cacao_ratings()
    if not df.empty:
        print("✅ Scraping réussi!")
        print(f"Shape: {df.shape}")
        print("Colonnes:", df.columns.tolist())
        print("\nAperçu des données:")
        print(df.head(10))
    else:
        print("❌ Scraping échoué")
