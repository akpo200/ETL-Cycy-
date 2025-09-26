"""
Module pour le chargement des données dans PostgreSQL.
Utilise SQLAlchemy pour une connexion générique et sécurisée.
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional, Dict
import logging
from ..config.config import config

logger = logging.getLogger(__name__)

class PostgreSQLLoader:
    """Classe pour le chargement de données dans PostgreSQL."""

    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialise le loader avec une chaîne de connexion.

        Args:
            connection_string (str): Chaîne de connexion SQLAlchemy
        """
        self.connection_string = connection_string or config.database_url
        self.engine = None
        self._connect()

    def _connect(self):
        """Établit la connexion à la base de données PostgreSQL."""
        try:
            self.engine = create_engine(self.connection_string)
            # Test de connexion
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Connexion à PostgreSQL établie avec succès")
        except SQLAlchemyError as e:
            logger.error(f"Erreur de connexion à PostgreSQL: {e}")
            raise

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
        index: bool = False,
        dtype: Optional[Dict] = None,
        chunksize: int = 1000
    ) -> bool:
        """Charge un DataFrame pandas dans une table PostgreSQL."""
        if df.empty:
            logger.warning("⚠️ Le DataFrame est vide, aucun chargement effectué.")
            return False

        try:
            if self.engine is None:
                self._connect()

            df.to_sql(
                table_name,
                self.engine,
                if_exists=if_exists,
                index=index,
                dtype=dtype,
                method='multi',
                chunksize=chunksize
            )
            logger.info(f"DataFrame chargé dans la table '{table_name}': {df.shape[0]} lignes")
            return True

        except SQLAlchemyError as e:
            logger.error(f"Erreur lors du chargement dans PostgreSQL: {e}")
            return False

    def create_table_from_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_key: Optional[str] = None
    ) -> bool:
        """Crée une table optimisée à partir d'un DataFrame."""
        try:
            if self.engine is None:
                self._connect()

            dtype_mapping = self._get_sql_dtypes(df)

            if primary_key and primary_key in df.columns:
                dtype_mapping[primary_key] = f"{dtype_mapping[primary_key]} PRIMARY KEY"

            df.head(0).to_sql(
                table_name,
                self.engine,
                if_exists='fail',
                index=False,
                dtype=dtype_mapping
            )

            logger.info(f"Table '{table_name}' créée avec succès")
            return True

        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de la création de la table: {e}")
            return False

    def _get_sql_dtypes(self, df: pd.DataFrame) -> Dict[str, str]:
        """Map les types pandas vers les types PostgreSQL optimisés."""
        type_mapping = {}
        for column in df.columns:
            dtype = df[column].dtype

            if pd.api.types.is_integer_dtype(dtype):
                if df[column].min() >= -32768 and df[column].max() <= 32767:
                    type_mapping[column] = "SMALLINT"
                elif df[column].min() >= -2147483648 and df[column].max() <= 2147483647:
                    type_mapping[column] = "INTEGER"
                else:
                    type_mapping[column] = "BIGINT"

            elif pd.api.types.is_float_dtype(dtype):
                type_mapping[column] = "FLOAT"

            elif pd.api.types.is_datetime64_any_dtype(dtype):
                type_mapping[column] = "TIMESTAMP"

            elif pd.api.types.is_bool_dtype(dtype):
                type_mapping[column] = "BOOLEAN"

            else:
                max_length = df[column].astype(str).str.len().max()
                type_mapping[column] = "VARCHAR(255)" if max_length <= 255 else "TEXT"

        return type_mapping

    def execute_query(self, query: str) -> bool:
        """Exécute une requête SQL arbitraire."""
        try:
            if self.engine is None:
                self._connect()

            with self.engine.begin() as conn:  # begin() gère le commit automatiquement
                conn.execute(text(query))

            logger.info("Requête SQL exécutée avec succès")
            return True

        except SQLAlchemyError as e:
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            return False

    def close_connection(self):
        """Ferme la connexion à la base de données."""
        if self.engine:
            self.engine.dispose()
            logger.info("Connexion PostgreSQL fermée")
