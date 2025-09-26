"""
Module générique pour l'écriture de données dans différents formats.
Support pour CSV, Parquet, Excel, etc.
"""

import pandas as pd
import os
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DataWriter:
    """
    Classe générique pour l'écriture de données dans différents formats.
    """
    
    @staticmethod
    def write_csv(df: pd.DataFrame, 
                 file_path: str, 
                 **kwargs) -> bool:
        """
        Écrit un DataFrame dans un fichier CSV.
        
        Args:
            df (pd.DataFrame): DataFrame à écrire
            file_path (str): Chemin du fichier de destination
            **kwargs: Arguments supplémentaires pour pandas.to_csv()
            
        Returns:
            bool: True si succès, False sinon
        """
        try:
            # Créer le répertoire si nécessaire
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            df.to_csv(file_path, **kwargs)
            logger.info(f"DataFrame écrit dans {file_path}: {df.shape[0]} lignes")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture du CSV: {e}")
            return False
    
    @staticmethod
    def write_parquet(df: pd.DataFrame, 
                     file_path: str, 
                     **kwargs) -> bool:
        """
        Écrit un DataFrame dans un fichier Parquet.
        
        Args:
            df (pd.DataFrame): DataFrame à écrire
            file_path (str): Chemin du fichier de destination
            **kwargs: Arguments supplémentaires pour pandas.to_parquet()
            
        Returns:
            bool: True si succès, False sinon
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            df.to_parquet(file_path, **kwargs)
            logger.info(f"DataFrame écrit dans {file_path}: {df.shape[0]} lignes")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture du Parquet: {e}")
            return False
    
    @staticmethod
    def write_excel(df: pd.DataFrame, 
                   file_path: str, 
                   sheet_name: str = 'Sheet1',
                   **kwargs) -> bool:
        """
        Écrit un DataFrame dans un fichier Excel.
        
        Args:
            df (pd.DataFrame): DataFrame à écrire
            file_path (str): Chemin du fichier de destination
            sheet_name (str): Nom de la feuille
            **kwargs: Arguments supplémentaires pour pandas.to_excel()
            
        Returns:
            bool: True si succès, False sinon
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name=sheet_name, **kwargs)
            
            logger.info(f"DataFrame écrit dans {file_path}: {df.shape[0]} lignes")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture de l'Excel: {e}")
            return False
    
    @staticmethod
    def write_json(df: pd.DataFrame, 
                  file_path: str, 
                  orient: str = 'records',
                  **kwargs) -> bool:
        """
        Écrit un DataFrame dans un fichier JSON.
        
        Args:
            df (pd.DataFrame): DataFrame à écrire
            file_path (str): Chemin du fichier de destination
            orient (str): Format du JSON
            **kwargs: Arguments supplémentaires pour pandas.to_json()
            
        Returns:
            bool: True si succès, False sinon
        """
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            df.to_json(file_path, orient=orient, **kwargs)
            logger.info(f"DataFrame écrit dans {file_path}: {df.shape[0]} lignes")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture du JSON: {e}")
            return False