"""
Module pour l'ingénierie des features.
Création de nouvelles variables et transformations avancées.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class FeatureEngineer:
    """
    Classe pour la création et transformation de features.
    """
    
    @staticmethod
    def create_date_features(df: pd.DataFrame, 
                           date_column: str) -> pd.DataFrame:
        """
        Crée des features à partir d'une colonne date.
        
        Args:
            df (pd.DataFrame): DataFrame contenant la colonne date
            date_column (str): Nom de la colonne date
            
        Returns:
            pd.DataFrame: DataFrame avec les nouvelles features
        """
        df_engineered = df.copy()
        
        if date_column in df.columns:
            # Convertir en datetime si ce n'est pas déjà fait
            df_engineered[date_column] = pd.to_datetime(df[date_column])
            
            # Extraire différentes composantes temporelles
            df_engineered[f'{date_column}_year'] = df_engineered[date_column].dt.year
            df_engineered[f'{date_column}_month'] = df_engineered[date_column].dt.month
            df_engineered[f'{date_column}_day'] = df_engineered[date_column].dt.day
            df_engineered[f'{date_column}_dayofweek'] = df_engineered[date_column].dt.dayofweek
            df_engineered[f'{date_column}_quarter'] = df_engineered[date_column].dt.quarter
            df_engineered[f'{date_column}_is_weekend'] = df_engineered[date_column].dt.dayofweek.isin([5, 6]).astype(int)
            
            logger.info(f"Features temporelles créées à partir de {date_column}")
        
        return df_engineered
    
    @staticmethod
    def create_interaction_features(df: pd.DataFrame, 
                                  columns: List[str]) -> pd.DataFrame:
        """
        Crée des features d'interaction entre colonnes numériques.
        
        Args:
            df (pd.DataFrame): DataFrame source
            columns (list): Colonnes pour les interactions
            
        Returns:
            pd.DataFrame: DataFrame avec features d'interaction
        """
        df_engineered = df.copy()
        numeric_columns = [col for col in columns if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
        
        # Créer des interactions pour les paires de colonnes
        for i, col1 in enumerate(numeric_columns):
            for j, col2 in enumerate(numeric_columns):
                if i < j:  # Éviter les doublons
                    interaction_name = f"{col1}_x_{col2}"
                    df_engineered[interaction_name] = df[col1] * df[col2]
        
        logger.info(f"Features d'interaction créées pour {len(numeric_columns)} colonnes")
        return df_engineered
    
    @staticmethod
    def create_polynomial_features(df: pd.DataFrame, 
                                 columns: List[str], 
                                 degree: int = 2) -> pd.DataFrame:
        """
        Crée des features polynomiales pour les colonnes numériques.
        
        Args:
            df (pd.DataFrame): DataFrame source
            columns (list): Colonnes pour les transformations polynomiales
            degree (int): Degré polynomial maximum
            
        Returns:
            pd.DataFrame: DataFrame avec features polynomiales
        """
        df_engineered = df.copy()
        
        for column in columns:
            if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                for deg in range(2, degree + 1):
                    poly_name = f"{column}_pow_{deg}"
                    df_engineered[poly_name] = df[column] ** deg
        
        logger.info(f"Features polynomiales créées (degré {degree})")
        return df_engineered
    
    @staticmethod
    def create_binning_features(df: pd.DataFrame, 
                              columns: List[str], 
                              bins: int = 5, 
                              strategy: str = 'quantile') -> pd.DataFrame:
        """
        Crée des features catégorielles par binning des variables numériques.
        
        Args:
            df (pd.DataFrame): DataFrame source
            columns (list): Colonnes à binner
            bins (int): Nombre de bins
            strategy (str): Stratégie de binning ('quantile' ou 'uniform')
            
        Returns:
            pd.DataFrame: DataFrame avec features binnées
        """
        df_engineered = df.copy()
        
        for column in columns:
            if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                if strategy == 'quantile':
                    df_engineered[f'{column}_binned'] = pd.qcut(df[column], bins, labels=False, duplicates='drop')
                else:  # uniform
                    df_engineered[f'{column}_binned'] = pd.cut(df[column], bins, labels=False)
                
                logger.info(f"Colonne '{column}' binnée en {bins} catégories")
        
        return df_engineered
    
    @staticmethod
    def create_aggregation_features(df: pd.DataFrame, 
                                  groupby_columns: List[str], 
                                  agg_columns: List[str],
                                  agg_functions: List[str] = ['mean', 'std', 'min', 'max']) -> pd.DataFrame:
        """
        Crée des features d'agrégation par groupe.
        
        Args:
            df (pd.DataFrame): DataFrame source
            groupby_columns (list): Colonnes de regroupement
            agg_columns (list): Colonnes à agréger
            agg_functions (list): Fonctions d'agrégation
            
        Returns:
            pd.DataFrame: DataFrame avec features agrégées
        """
        df_engineered = df.copy()
        
        # Vérifier que les colonnes existent
        valid_groupby = [col for col in groupby_columns if col in df.columns]
        valid_agg = [col for col in agg_columns if col in df.columns and pd.api.types.is_numeric_dtype(df[col])]
        
        if valid_groupby and valid_agg:
            # Calculer les agrégations
            aggregation = df.groupby(valid_groupby)[valid_agg].agg(agg_functions)
            aggregation.columns = [f"{col}_{func}" for col, func in aggregation.columns]
            aggregation = aggregation.reset_index()
            
            # Fusionner avec le DataFrame original
            df_engineered = df_engineered.merge(aggregation, on=valid_groupby, how='left')
            
            logger.info(f"Features d'agrégation créées pour {len(valid_agg)} colonnes")
        
        return df_engineered