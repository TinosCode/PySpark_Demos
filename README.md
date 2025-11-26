# PySpark_Demos
Dieses Repository enthält zwei PySpark-Notebooks (exportiert als Python-Quelldateien). Die Projekte wurden in einer Databricks-Umgebung entwickelt und demonstrieren grundlegende Data Engineering-Fähigkeiten im Umgang mit DataFrames, Datenbereinigung, API-Integration und ETL-Prozessen.

## Projekte im Überblick
### 01_read_group_store_csv.py
Dieses Skript liest eine Beispieldatei ('customers-1000.csv') ein und führt folgende Schritte durch:
  1. Datenaufnahme: Laden einer CSV-Datei in einen PySpark DataFrame mit automatischer Schemaerkennung (inferSchema).
  2. Datenbereinigung & Transformation: Definition einer Python-Funktion, die Spark SQL-Funktionen (lower, split, element_at) nutzt, um Domain-Suffixe aus E-Mail- und Website-Spalten zu extrahieren. Erstellung neuer Spalten (email_suffix, website_suffix).
  3. Analyse: Filtern nach Datensätzen, bei denen E-Mail- und Website-Suffix übereinstimmen.
  4. Aggregation: Gruppierung und Zählung der Übereinstimmungen.
  5. Datenspeicherung: Speichern des Endergebnisses als Delta Lake-Tabelle (testcsvs_suffix_matches).

Voraussetzung: Die Quelldatei muss im Databricks Volume Pfad /Volumes/workspace/testcsvs/testcsvs/customers-1000.csv verfügbar sein.
### 02_api_weather_test.py
Dieses Skript demonstriert einen ETL-Flow, der externe APIs einbindet:
  1. Extraktion: Abfrage der Open-Meteo-API für Wetterdaten verschiedener europäischer Städte mittels der requests-Bibliothek.
  2. Transformation: Umwandlung der verschachtelten JSON-API-Antwort in einen flachen DataFrame. Nutzung von explode und posexplode zur korrekten Zuordnung von Zeitstempeln und Messwerten mittels Index-basiertem Join. Konvertierung von Einheiten (Celsius nach Fahrenheit, m/s nach km/h) und Datentypanpassungen.
  3. Laden & Aggregation: Aggregierung der stündlichen Daten zu täglichen Durchschnittswerten und Speichern als Delta Lake-Tabelle ('testcsvs_weather').
