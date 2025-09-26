# ETL Package Modulaire

Un package Python complet et modulaire pour les projets ETL (Extract, Transform, Load). Conçu pour être réutilisable et adaptable à différents cas d'usage.

## 🚀 Fonctionnalités

- **Extraction** : Web scraping, fichiers (CSV, Excel, JSON), APIs
- **Transformation** : Gestion des valeurs manquantes, encodage, validation, feature engineering
- **Chargement** : PostgreSQL, CSV, Parquet, Excel, JSON
- **Modulaire** : Architecture claire et extensible
- **Configurable** : Paramètres via variables d'environnement


## 📦 Installation

1. Cloner le repository :
```bash
git clone <repository-url>
cd etl_package

#

## 🔧 Personnalisation
Ajouter une nouvelle source d'extraction
Créer une nouvelle classe dans extract/

Implémenter les méthodes nécessaires

Modifier ETLPipeline.extract()

Ajouter une nouvelle transformation
Créer un module dans transform/

Implémenter les fonctions de transformation

Ajouter au pipeline dans transformations

Changer la destination
Modifier le paramètre destination dans run_pipeline() :

postgres : Base de données PostgreSQL

csv : Fichier CSV

parquet : Fichier Parquet