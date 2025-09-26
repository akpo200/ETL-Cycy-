"""
Module pour l'encodage des variables catégorielles.
Implémente one-hot encoding et label encoding de manière générique.
"""

import pandas as pd
from sklearn.preprocessing import LabelEncoder
from typing import List, Optional, Dict
import logging

logger = logging.getLogger(__name__)

class DataEncoder:
    """
    Classe pour l'encodage des variables catégorielles.
    """
    
    @staticmethod
    def one_hot_encode(df: pd.DataFrame, 
                      columns: Optional[List[str]] = None,
                      drop_first: bool = False) -> pd.DataFrame:
        """
        Encode les variables catégorielles en one-hot encoding.
        
        Args:
            df (pd.DataFrame): DataFrame à encoder
            columns (list): Colonnes à encoder (toutes les catégorielles par défaut)
            drop_first (bool): Supprimer la première colonne pour éviter la colinéarité
            
        Returns:
            pd.DataFrame: DataFrame encodé
        """
        df_encoded = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        for column in columns:
            if column in df.columns:
                dummies = pd.get_dummies(df[column], prefix=column, drop_first=drop_first)
                df_encoded = pd.concat([df_encoded, dummies], axis=1)
                df_encoded.drop(column, axis=1, inplace=True)
                logger.info(f"Colonne '{column}' encodée en one-hot ({len(dummies.columns)} nouvelles colonnes)")
        
        return df_encoded
    
    @staticmethod
    def label_encode(df: pd.DataFrame, 
                    columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Encode les variables catégorielles en labels numériques.
        
        Args:
            df (pd.DataFrame): DataFrame à encoder
            columns (list): Colonnes à encoder
            
        Returns:
            pd.DataFrame: DataFrame encodé
        """
        df_encoded = df.copy()
        label_encoder = LabelEncoder()
        
        if columns is None:
            columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        for column in columns:
            if column in df.columns:
                df_encoded[column] = label_encoder.fit_transform(df[column].astype(str))
                logger.info(f"Colonne '{column}' encodée en labels ({len(label_encoder.classes_)} classes)")
        
        return df_encoded
    
    @staticmethod
    def frequency_encode(df: pd.DataFrame, 
                        columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Encode les variables catégorielles par fréquence d'apparition.
        
        Args:
            df (pd.DataFrame): DataFrame à encoder
            columns (list): Colonnes à encoder
            
        Returns:
            pd.DataFrame: DataFrame encodé
        """
        df_encoded = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=['object', 'category']).columns.tolist()
        
        for column in columns:
            if column in df.columns:
                freq_map = df[column].value_counts(normalize=True).to_dict()
                df_encoded[column] = df[column].map(freq_map)
                logger.info(f"Colonne '{column}' encodée par fréquence")
        
        return df_encoded