 ETL Package Modulaire

Un package Python conçu pour faciliter le développement de pipelines **ETL (Extract – Transform – Load)**.
Pensé pour être **réutilisable, extensible et adaptable** à différents contextes de traitement de données.

 Points forts

 **Extraction** : récupération de données depuis des sites web (web scraping), fichiers plats (CSV, Excel, JSON) ou APIs.
 **Transformation** : nettoyage des données, gestion des valeurs manquantes, encodage, validation et enrichissement (feature engineering).
 **Chargement** : export vers PostgreSQL, CSV, Parquet ou Excel/JSON.
 **Architecture modulaire** : organisation claire des modules facilitant la maintenance et l’extension.
 **Configuration flexible** : paramètres ajustables via variables d’environnement.

 Installation

Clonez le projet et placez-vous dans le dossier :

```bash
git clone <repository-url>
cd etl_package
```


 Personnalisation et extensions

 Ajouter une nouvelle source d’extraction

* Créer une classe dédiée dans le répertoire `extract/`.
* Implémenter les méthodes nécessaires.
* Mettre à jour la méthode `ETLPipeline.extract()`.

Intégrer une nouvelle transformation

* Créer un module dans `transform/`.
* Implémenter la ou les fonctions de transformation.
* Ajouter la transformation au pipeline dans la section `transformations`.

 Modifier la destination des données

Dans la fonction `run_pipeline()`, définir le paramètre `destination` :

 **postgres** : envoi vers une base PostgreSQL.
 **csv** : export en fichier CSV.
 **parquet** : génération d’un fichier Parquet.
 **excel/json** : export vers Excel ou JSON.
