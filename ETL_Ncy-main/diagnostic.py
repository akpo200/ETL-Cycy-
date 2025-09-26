#!/usr/bin/env python3
"""
Script de diagnostic pour identifier la structure
"""

import os
import sys

print("=== DIAGNOSTIC COMPLET ===")
print(f"Dossier courant: {os.getcwd()}")
print(f"Python path: {sys.executable}")

print("\n📁 Structure du dossier:")
for item in os.listdir('.'):
    item_path = os.path.join('.', item)
    if os.path.isdir(item_path):
        print(f"📁 {item}/")
        # Afficher le contenu des dossiers importants
        if item in ['etl_package', 'PACKAGE_ETL_FADEL', 'config', 'extract', 'transform', 'load']:
            try:
                for subitem in os.listdir(item_path):
                    print(f"    📄 {subitem}")
            except Exception as e:
                print(f"    ❌ Erreur: {e}")
    else:
        print(f"📄 {item}")

print("\n🔍 Recherche des dossiers importants:")
important_dirs = ['etl_package', 'PACKAGE_ETL_FADEL', 'config']
for dir_name in important_dirs:
    if os.path.exists(dir_name):
        print(f"✅ '{dir_name}' trouvé!")
        print(f"   Contenu: {os.listdir(dir_name)}")
    else:
        print(f"❌ '{dir_name}' introuvable")

print("\n📋 Contenu du setup.py (si existe):")
if os.path.exists('setup.py'):
    with open('setup.py', 'r') as f:
        content = f.read()
        # Chercher le nom du package
        if 'name=' in content:
            start = content.find('name=') + 5
            end = content.find(',', start)
            package_name = content[start:end].strip().strip('"').strip("'")
            print(f"   Nom du package dans setup.py: {package_name}")
else:
    print("   setup.py introuvable")

print("\n🔄 Test des imports:")
# Test 1: etl_package
try:
    from etl_package.config import config
    print("✅ Import etl_package réussi!")
except ImportError as e:
    print(f"❌ Import etl_package échoué: {e}")

# Test 2: Vérifier si config existe à la racine
try:
    from config.config import config
    print("✅ Import config direct réussi!")
except ImportError as e:
    print(f"❌ Import config direct échoué: {e}")

# Test 3: Vérifier le PYTHONPATH
print(f"\n📊 PYTHONPATH:")
for path in sys.path:
    print(f"   {path}")

print("\n=== FIN DU DIAGNOSTIC ===")