"""
Module de configuration du logging pour l'ensemble du package.
Fournit une configuration centralisée des logs.
"""

import logging
import sys
from pathlib import Path
from ..config.config import config

def setup_logger(name: str = 'etl_package', log_level: str = None) -> logging.Logger:
    """
    Configure et retourne un logger avec une configuration standard.
    
    Args:
        name (str): Nom du logger
        log_level (str): Niveau de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        
    Returns:
        logging.Logger: Logger configuré
    """
    log_level = log_level or config.LOG_LEVEL
    
    # Créer le logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Éviter les handlers multiples
    if logger.handlers:
        logger.handlers.clear()
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Handler fichier
    log_file = Path('logs/etl_package.log')
    log_file.parent.mkdir(exist_ok=True)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger