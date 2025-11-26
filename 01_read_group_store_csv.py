# Databricks notebook source
from pyspark.sql.functions import col, split, element_at, lower

# Einlesem des CSV-Datensatzes
df = (spark.read.format("csv")
      .option("header", "true")
     .option("inferSchema", "true")
     .load("/Volumes/workspace/testcsvs/testcsvs/customers-1000.csv"))

# Funktionsdefinition für die Extraktion des letzten Suffixes
def get_last_suffix(column_name):
    # Alles in Kleinbuchstaben umwandeln für den Vergleich
    lower_col = lower(col(column_name))
    # Den String beim Punkt trennen und eine Array-Spalte erstellen
    split_array = split(lower_col, "\\.") # Wir müssen den Punkt escapen
    # Das allerletzte Element des Arrays auswählen (Index -1)
    suffix_with_slash = element_at(split_array, -1)
    suffix_clean = element_at(split(suffix_with_slash, "/"), 1)
    return suffix_clean

# Anwenden der Suffix-Funktion auf die beiden Spalten
df_processed = df.withColumn("email_suffix", get_last_suffix("Email"))
df_processed = df_processed.withColumn("website_suffix", get_last_suffix("website"))

# Filtern des DataFrames nach den übereinstimmenden Suffixen
df_filtered = df_processed.filter(
    (col("email_suffix") == col("website_suffix")) &
    (col("email_suffix").isNotNull()) &
    (col("email_suffix") != "")
)

# Gruppiere nach dem Suffix und zähle die Anzahl
df_filtered = df_filtered.groupBy("email_suffix").count().sort("count", ascending=False)
# Änderung des Spaltennamens email_suffix zu suffix_match
df_renamed = df_filtered.withColumnRenamed("email_suffix", "suffix_match")

# Speichern des DataFrames als Delta-Tabelle
df_renamed.write.format("delta").mode("overwrite").saveAsTable("testcsvs_suffix_matches")
