#!/usr/bin/env python3
"""
Réorganisation de la structure des dossiers
"""

import os
import shutil
from pathlib import Path

def reorganize_structure():
    print("🔧 Réorganisation de la structure...")
    
    # Chemins importants
    base_dir = Path('etl_package')
    config_file = base_dir / 'config.py'
    
    # 1. Créer les dossiers manquants
    folders = ['config', 'extract', 'transform', 'load', 'pipelines', 'utils']
    for folder in folders:
        (base_dir / folder).mkdir(exist_ok=True)
        # Créer les __init__.py
        init_file = base_dir / folder / '__init__.py'
        init_file.write_text('# ' + folder + ' module\n')
    
    # 2. Déplacer config.py dans config/
    if config_file.exists():
        shutil.move(str(config_file), str(base_dir / 'config' / 'config.py'))
        print("✅ config.py déplacé dans etl_package/config/")
    
    # 3. Créer les fichiers manquants dans chaque dossier
    # config/
    config_init = base_dir / 'config' / '__init__.py'
    config_init.write_text('''
"""
Module de configuration
"""
from .config import config
__all__ = ['config']
''')
    
    # extract/
    extract_init = base_dir / 'extract' / '__init__.py'
    extract_init.write_text('''
"""
Modules d'extraction
"""
from .web_scraper import scrape_cacao_ratings, WebScraper
from .data_loader import DataLoader
__all__ = ['scrape_cacao_ratings', 'WebScraper', 'DataLoader']
''')
    
    # transform/
    transform_init = base_dir / 'transform' / '__init__.py'
    transform_init.write_text('''
"""
Modules de transformation
"""
from .missing_values import MissingValuesHandler
from .encoding import DataEncoder
from .validation import DataValidator
from .feature_engineering import FeatureEngineer
from .scaling import DataScaler
__all__ = ['MissingValuesHandler', 'DataEncoder', 'DataValidator', 'FeatureEngineer', 'DataScaler']
''')
    
    # load/
    load_init = base_dir / 'load' / '__init__.py'
    load_init.write_text('''
"""
Modules de chargement
"""
from .to_postgres import PostgreSQLLoader
from .data_writer import DataWriter
__all__ = ['PostgreSQLLoader', 'DataWriter']
''')
    
    # pipelines/
    pipelines_init = base_dir / 'pipelines' / '__init__.py'
    pipelines_init.write_text('''
"""
Pipelines ETL
"""
from .etl_pipeline import ETLPipeline, run_pipeline, main
__all__ = ['ETLPipeline', 'run_pipeline', 'main']
''')
    
    # utils/
    utils_init = base_dir / 'utils' / '__init__.py'
    utils_init.write_text('''
"""
Utilitaires
"""
from .logger import setup_logger
__all__ = ['setup_logger']
''')
    
    print("✅ Structure réorganisée avec succès!")
    
    # Afficher la nouvelle structure
    print("\n📁 Nouvelle structure:")
    for root, dirs, files in os.walk('etl_package'):
        level = root.replace('etl_package', '').count(os.sep)
        indent = '  ' * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = '  ' * (level + 1)
        for file in files:
            if file.endswith('.py'):
                print(f"{subindent}{file}")

if __name__ == "__main__":
    reorganize_structure()