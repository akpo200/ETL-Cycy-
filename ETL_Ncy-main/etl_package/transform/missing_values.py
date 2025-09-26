"""
Module pour la gestion des valeurs manquantes.
Détection et imputation des valeurs manquantes avec différentes stratégies.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Union, Optional
import logging

logger = logging.getLogger(__name__)

class MissingValuesHandler:
    """
    Classe pour la détection et l'imputation des valeurs manquantes.
    Générique et applicable à n'importe quel DataFrame.
    """
    
    @staticmethod
    def detect_missing_values(df: pd.DataFrame) -> pd.DataFrame:
        """
        Détecte et retourne un résumé des valeurs manquantes par colonne.
        
        Args:
            df (pd.DataFrame): DataFrame à analyser
            
        Returns:
            pd.DataFrame: Résumé des valeurs manquantes
        """
        missing_sum = df.isnull().sum()
        missing_percent = (df.isnull().sum() / len(df)) * 100
        
        missing_info = pd.DataFrame({
            'Colonne': df.columns,
            'Valeurs_manquantes': missing_sum.values,
            'Pourcentage_manquant': missing_percent.values
        })
        
        logger.info("Détection des valeurs manquantes terminée")
        return missing_info.sort_values('Valeurs_manquantes', ascending=False)
    
    @staticmethod
    def impute_missing_values(df: pd.DataFrame, 
                            strategy: Dict[str, Union[str, float]] = None,
                            default_strategy: str = 'mean') -> pd.DataFrame:
        """
        Impute les valeurs manquantes selon différentes stratégies.
        
        Args:
            df (pd.DataFrame): DataFrame avec valeurs manquantes
            strategy (dict): Stratégie par colonne {'colonne': 'strategy'}
            default_strategy (str): Stratégie par défaut
            
        Returns:
            pd.DataFrame: DataFrame avec valeurs imputées
        """
        df_imputed = df.copy()
        
        if strategy is None:
            strategy = {}
        
        for column in df.columns:
            if df[column].isnull().sum() > 0:
                col_strategy = strategy.get(column, default_strategy)
                
                if col_strategy == 'mean' and pd.api.types.is_numeric_dtype(df[column]):
                    impute_value = df[column].mean()
                elif col_strategy == 'median' and pd.api.types.is_numeric_dtype(df[column]):
                    impute_value = df[column].median()
                elif col_strategy == 'mode':
                    impute_value = df[column].mode()[0] if not df[column].mode().empty else None
                elif col_strategy == 'ffill':
                    df_imputed[column] = df[column].fillna(method='ffill')
                    continue
                elif col_strategy == 'bfill':
                    df_imputed[column] = df[column].fillna(method='bfill')
                    continue
                elif isinstance(col_strategy, (int, float)):
                    impute_value = col_strategy
                else:
                    impute_value = df[column].mode()[0] if not df[column].mode().empty else None
                
                df_imputed[column] = df[column].fillna(impute_value)
                logger.info(f"Colonne '{column}' imputée avec stratégie: {col_strategy}")
        
        logger.info("Imputation des valeurs manquantes terminée")
        return df_imputed
    
    @staticmethod
    def drop_missing_values(df: pd.DataFrame, 
                          threshold: float = 0.5,
                          axis: int = 0) -> pd.DataFrame:
        """
        Supprime les lignes ou colonnes avec trop de valeurs manquantes.
        
        Args:
            df (pd.DataFrame): DataFrame à nettoyer
            threshold (float): Seuil de pourcentage de valeurs manquantes
            axis (int): 0 pour lignes, 1 pour colonnes
            
        Returns:
            pd.DataFrame: DataFrame nettoyé
        """
        if axis == 0:  # Supprimer les lignes
            threshold_count = int(threshold * df.shape[1])
            df_cleaned = df.dropna(thresh=threshold_count)
        else:  # Supprimer les colonnes
            threshold_count = int(threshold * df.shape[0])
            df_cleaned = df.dropna(axis=1, thresh=threshold_count)
        
        logger.info(f"Données nettoyées: {df.shape} -> {df_cleaned.shape}")
        return df_cleaned