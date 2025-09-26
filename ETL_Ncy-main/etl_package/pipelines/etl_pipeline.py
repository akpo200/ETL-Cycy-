"""
Pipeline ETL principal pour l'orchestration complète du processus.
Exemple concret avec le dataset cacao ratings.
"""

from etl_package.extract.web_scraper import scrape_cacao_ratings
from etl_package.transform.missing_values import MissingValuesHandler
from etl_package.transform.encoding import DataEncoder
from etl_package.transform.validation import DataValidator
from etl_package.transform.feature_engineering import FeatureEngineer
from etl_package.load.to_postgres import PostgreSQLLoader
from etl_package.config.config import config
import logging
import pandas as pd

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ETLPipeline:
    """
    Classe principale pour l'orchestration du pipeline ETL.
    Modulaire et réutilisable pour différents projets.
    """
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.raw_data = None
        self.transformed_data = None
        self.loader = None
        logger.info("Pipeline ETL initialisé")

    def extract(self, source: str = 'web_scraping', **kwargs) -> pd.DataFrame:
        logger.info(f"Début de la phase d'extraction - Source: {source}")
        try:
            if source == 'web_scraping':
                self.raw_data = scrape_cacao_ratings()
            else:
                raise ValueError(f"Source non supportée: {source}")
            
            logger.info(f"Extraction terminée: {self.raw_data.shape[0]} lignes, {self.raw_data.shape[1]} colonnes")
            return self.raw_data
        except Exception as e:
            logger.error(f"Erreur lors de l'extraction: {e}")
            raise

    def transform(self, transformations: list) -> pd.DataFrame:
        if self.raw_data is None:
            raise ValueError("Aucune donnée à transformer. Exécutez d'abord extract().")
        
        logger.info("Début de la phase de transformation")
        self.transformed_data = self.raw_data.copy()
        try:
            for transformation in transformations:
                func_name = transformation['function']
                params = transformation.get('params', {})

                logger.info(f"Application de la transformation: {func_name}")
                if func_name == 'handle_missing_values':
                    self.transformed_data = MissingValuesHandler.impute_missing_values(
                        self.transformed_data, **params
                    )
                elif func_name == 'encode_categorical':
                    self.transformed_data = DataEncoder.one_hot_encode(
                        self.transformed_data, **params
                    )
                elif func_name == 'validate_data':
                    validator = DataValidator()
                    result = validator.validate_data_types(self.transformed_data, **params)
                    if not result['valid']:
                        logger.warning(f"Problèmes de validation: {result['errors']}")
                elif func_name == 'create_features':
                    self.transformed_data = FeatureEngineer.create_date_features(
                        self.transformed_data, **params
                    )
                else:
                    logger.warning(f"Transformation non reconnue: {func_name}")
            
            logger.info(f"Transformation terminée: {self.transformed_data.shape[1]} colonnes")
            return self.transformed_data
        except Exception as e:
            logger.error(f"Erreur lors de la transformation: {e}")
            raise

    def load(self, destination: str = 'postgres', **kwargs) -> bool:
        if self.transformed_data is None:
            raise ValueError("Aucune donnée à charger. Exécutez d'abord transform().")
        
        logger.info(f"Début de la phase de chargement - Destination: {destination}")
        try:
            if destination == 'postgres':
                table_name = kwargs.get('table_name', 'cacao_ratings')
                self.loader = PostgreSQLLoader()
                success = self.loader.load_dataframe(
                    self.transformed_data, 
                    table_name=table_name,
                    if_exists=kwargs.get('if_exists', 'replace')
                )
            else:
                raise ValueError(f"Destination non supportée: {destination}")
            
            if success:
                logger.info("Chargement terminé avec succès")
            else:
                logger.error("Échec du chargement")
            return success
        except Exception as e:
            logger.error(f"Erreur lors du chargement: {e}")
            raise

    def run_pipeline(self, 
                     source: str = 'web_scraping',
                     transformations: list = None,
                     destination: str = 'postgres',
                     source_kwargs: dict = None,
                     load_kwargs: dict = None) -> bool:
        logger.info("Démarrage du pipeline ETL complet")
        try:
            self.extract(source, **(source_kwargs or {}))
            
            if transformations is None:
                transformations = [
                    {'function': 'handle_missing_values', 'params': {'strategy': {}, 'default_strategy': 'mean'}},
                    {'function': 'encode_categorical', 'params': {'columns': None, 'drop_first': True}},
                    {'function': 'validate_data', 'params': {}}
                ]
            
            self.transform(transformations)
            success = self.load(destination, **(load_kwargs or {}))
            
            if success:
                logger.info("Pipeline ETL exécuté avec succès")
            else:
                logger.error("Pipeline ETL terminé avec des erreurs")
            return success
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution du pipeline: {e}")
            return False

# --- Fonctions globales pour import simplifié ---

def run_pipeline(*args, **kwargs):
    """
    Fonction utilitaire pour exécuter rapidement le pipeline ETL.
    """
    pipeline = ETLPipeline()
    return pipeline.run_pipeline(*args, **kwargs)

def main():
    """
    Fonction principale pour l'exécution du pipeline en ligne de commande.
    """
    logger.info("Démarrage du pipeline ETL Cacao Ratings")
    
    pipeline = ETLPipeline()
    cacao_transformations = [
        {'function': 'handle_missing_values', 'params': {'strategy': {}, 'default_strategy': 'mean'}},
    ]
    success = pipeline.run_pipeline(
        source='web_scraping',
        transformations=cacao_transformations,
        destination='postgres',
        load_kwargs={'table_name': 'cacao_ratings_processed'}
    )
    
    if success:
        logger.info("Pipeline Cacao Ratings terminé avec succès")
    else:
        logger.error("Pipeline Cacao Ratings a échoué")
    
    return success

if __name__ == "__main__":
    main()
