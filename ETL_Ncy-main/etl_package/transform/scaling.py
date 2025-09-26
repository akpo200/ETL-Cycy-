"""
Module pour la normalisation et standardisation des données.
Implémente MinMaxScaler et StandardScaler de manière générique.
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

class DataScaler:
    """
    Classe pour la mise à l'échelle des données numériques.
    """
    
    @staticmethod
    def min_max_scale(df: pd.DataFrame, 
                     columns: Optional[List[str]] = None,
                     feature_range: tuple = (0, 1)) -> pd.DataFrame:
        """
        Normalise les données entre une plage spécifiée (par défaut 0-1).
        
        Args:
            df (pd.DataFrame): DataFrame à normaliser
            columns (list): Colonnes à normaliser (toutes les numériques par défaut)
            feature_range (tuple): Plage de normalisation
            
        Returns:
            pd.DataFrame: DataFrame normalisé
        """
        df_scaled = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        scaler = MinMaxScaler(feature_range=feature_range)
        
        for column in columns:
            if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                # Reshape pour sklearn
                values = df[column].values.reshape(-1, 1)
                scaled_values = scaler.fit_transform(values)
                df_scaled[column] = scaled_values.flatten()
                logger.info(f"Colonne '{column}' normalisée avec MinMaxScaler")
        
        return df_scaled
    
    @staticmethod
    def standard_scale(df: pd.DataFrame, 
                      columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Standardise les données (moyenne=0, écart-type=1).
        
        Args:
            df (pd.DataFrame): DataFrame à standardiser
            columns (list): Colonnes à standardiser
            
        Returns:
            pd.DataFrame: DataFrame standardisé
        """
        df_scaled = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        scaler = StandardScaler()
        
        for column in columns:
            if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                values = df[column].values.reshape(-1, 1)
                scaled_values = scaler.fit_transform(values)
                df_scaled[column] = scaled_values.flatten()
                logger.info(f"Colonne '{column}' standardisée avec StandardScaler")
        
        return df_scaled
    
    @staticmethod
    def robust_scale(df: pd.DataFrame, 
                    columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Échelle robuste utilisant les quartiles (résistant aux outliers).
        
        Args:
            df (pd.DataFrame): DataFrame à transformer
            columns (list): Colonnes à transformer
            
        Returns:
            pd.DataFrame: DataFrame transformé
        """
        df_scaled = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        for column in columns:
            if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                
                if IQR != 0:
                    df_scaled[column] = (df[column] - df[column].median()) / IQR
                    logger.info(f"Colonne '{column}' transformée avec RobustScaler")
        
        return df_scaled