"""
Module pour la validation des données.
Vérification des types, cohérence des colonnes et qualité des données.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """
    Classe pour la validation et la vérification de la qualité des données.
    """
    
    @staticmethod
    def validate_data_types(df: pd.DataFrame, 
                          expected_types: Dict[str, type]) -> Dict[str, Any]:
        """
        Valide les types de données des colonnes.
        
        Args:
            df (pd.DataFrame): DataFrame à valider
            expected_types (dict): Types attendus {'colonne': type}
            
        Returns:
            dict: Résultats de la validation
        """
        validation_results = {
            'valid': True,
            'errors': [],
            'details': {}
        }
        
        for column, expected_type in expected_types.items():
            if column not in df.columns:
                validation_results['valid'] = False
                validation_results['errors'].append(f"Colonne manquante: {column}")
                validation_results['details'][column] = 'MANQUANTE'
            else:
                actual_type = df[column].dtype
                if not pd.api.types.is_dtype_equal(actual_type, expected_type):
                    validation_results['valid'] = False
                    validation_results['errors'].append(
                        f"Type incorrect pour {column}: attendu {expected_type}, obtenu {actual_type}"
                    )
                    validation_results['details'][column] = f"INVALID: {actual_type}"
                else:
                    validation_results['details'][column] = f"VALID: {actual_type}"
        
        logger.info(f"Validation des types: {validation_results['valid']}")
        return validation_results
    
    @staticmethod
    def check_missing_values(df: pd.DataFrame, threshold: float = 0.8) -> Dict[str, Any]:
        """
        Vérifie les valeurs manquantes et retourne un rapport.
        
        Args:
            df (pd.DataFrame): DataFrame à vérifier
            threshold (float): Seuil d'acceptation pour les valeurs manquantes
            
        Returns:
            dict: Rapport des valeurs manquantes
        """
        missing_count = df.isnull().sum()
        missing_percent = (missing_count / len(df)) * 100
        
        report = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_summary': {
                'columns_above_threshold': [],
                'columns_below_threshold': []
            },
            'details': {}
        }
        
        for column in df.columns:
            percent_missing = missing_percent[column]
            report['details'][column] = {
                'missing_count': missing_count[column],
                'missing_percent': percent_missing,
                'status': 'ACCEPTABLE' if percent_missing <= threshold * 100 else 'CRITIQUE'
            }
            
            if percent_missing > threshold * 100:
                report['missing_summary']['columns_above_threshold'].append(column)
            else:
                report['missing_summary']['columns_below_threshold'].append(column)
        
        logger.info(f"Vérification des valeurs manquantes terminée")
        return report
    
    @staticmethod
    def validate_value_ranges(df: pd.DataFrame, 
                            value_ranges: Dict[str, tuple]) -> Dict[str, Any]:
        """
        Valide les plages de valeurs pour les colonnes numériques.
        
        Args:
            df (pd.DataFrame): DataFrame à valider
            value_ranges (dict): Plages attendues {'colonne': (min, max)}
            
        Returns:
            dict: Résultats de la validation
        """
        validation_results = {
            'valid': True,
            'errors': [],
            'out_of_range': {}
        }
        
        for column, (min_val, max_val) in value_ranges.items():
            if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                out_of_range = df[(df[column] < min_val) | (df[column] > max_val)]
                count_out_of_range = len(out_of_range)
                
                if count_out_of_range > 0:
                    validation_results['valid'] = False
                    validation_results['errors'].append(
                        f"{count_out_of_range} valeurs hors plage pour {column} ({min_val}-{max_val})"
                    )
                    validation_results['out_of_range'][column] = {
                        'count': count_out_of_range,
                        'min_allowed': min_val,
                        'max_allowed': max_val,
                        'actual_min': df[column].min(),
                        'actual_max': df[column].max()
                    }
        
        logger.info(f"Validation des plages de valeurs: {validation_results['valid']}")
        return validation_results
    
    @staticmethod
    def check_duplicates(df: pd.DataFrame, subset: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Vérifie les doublons dans le DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame à vérifier
            subset (list): Sous-ensemble de colonnes pour la vérification
            
        Returns:
            dict: Rapport des doublons
        """
        duplicate_report = {
            'total_duplicates': 0,
            'duplicate_rows': pd.DataFrame(),
            'is_clean': True
        }
        
        if subset:
            duplicates = df[df.duplicated(subset=subset, keep=False)]
        else:
            duplicates = df[df.duplicated(keep=False)]
        
        duplicate_report['total_duplicates'] = len(duplicates)
        duplicate_report['duplicate_rows'] = duplicates
        duplicate_report['is_clean'] = len(duplicates) == 0
        
        if duplicate_report['total_duplicates'] > 0:
            logger.warning(f"{duplicate_report['total_duplicates']} doublons détectés")
        else:
            logger.info("Aucun doublon détecté")
        
        return duplicate_report