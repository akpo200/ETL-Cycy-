"""
Module générique pour charger des données depuis différentes sources
(CSV, Excel, API, etc.). Facilite le changement de source de données.
"""

import pandas as pd
import os
from typing import Union, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class DataLoader:
    """
    Classe générique pour charger des données depuis différentes sources.
    """
    
    @staticmethod
    def load_csv(file_path: str, **kwargs) -> pd.DataFrame:
        """
        Charge un fichier CSV dans un DataFrame pandas.
        
        Args:
            file_path (str): Chemin vers le fichier CSV
            **kwargs: Arguments supplémentaires pour pandas.read_csv()
            
        Returns:
            pd.DataFrame: Données chargées
        """
        try:
            logger.info(f"Chargement du fichier CSV: {file_path}")
            df = pd.read_csv(file_path, **kwargs)
            logger.info(f"Fichier chargé: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            return df
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {file_path}: {e}")
            raise
    
    @staticmethod
    def load_excel(file_path: str, sheet_name: Union[str, int] = 0, **kwargs) -> pd.DataFrame:
        """
        Charge un fichier Excel dans un DataFrame pandas.
        
        Args:
            file_path (str): Chemin vers le fichier Excel
            sheet_name (str/int): Nom ou index de la feuille
            **kwargs: Arguments supplémentaires pour pandas.read_excel()
            
        Returns:
            pd.DataFrame: Données chargées
        """
        try:
            logger.info(f"Chargement du fichier Excel: {file_path}")
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            logger.info(f"Fichier chargé: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            return df
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {file_path}: {e}")
            raise
    
    @staticmethod
    def load_json(file_path: str, **kwargs) -> pd.DataFrame:
        """
        Charge un fichier JSON dans un DataFrame pandas.
        
        Args:
            file_path (str): Chemin vers le fichier JSON
            **kwargs: Arguments supplémentaires pour pandas.read_json()
            
        Returns:
            pd.DataFrame: Données chargées
        """
        try:
            logger.info(f"Chargement du fichier JSON: {file_path}")
            df = pd.read_json(file_path, **kwargs)
            logger.info(f"Fichier chargé: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            return df
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {file_path}: {e}")
            raise