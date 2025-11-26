# Databricks notebook source
import requests
import json
from pyspark.sql.functions import (
    explode,
    posexplode,
    col,
    lit,
    monotonically_increasing_id
)
from pyspark.sql import functions as F
from pyspark.sql import Row

# Festlegen der Städte und deren Koordinaten
cities = [
    {"city": "Berlin", "lat": 52.52, "lon": 13.41},
    {"city": "Paris", "lat": 48.85, "lon": 2.35},
    {"city": "Madrid", "lat": 40.42, "lon": -3.70},
    {"city": "Rome", "lat": 41.90, "lon": 12.49},
    {"city": "Vienna", "lat": 48.20, "lon": 16.37},
]

all_records = []

# Abrufen der Wetterdaten
for c in cities:
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={c['lat']}&longitude={c['lon']}"
        "&hourly=temperature_2m,relative_humidity_2m,windspeed_10m"
        "&timezone=auto"
    )
    r = requests.get(url)
    data = r.json()

    data["city"] = c["city"]
    data["lat"] = c["lat"]
    data["lon"] = c["lon"]

    all_records.append(data)

# Erstellen des DataFrames mit den zuvor abgerufenen Daten
df_raw = spark.createDataFrame(all_records)

# Für jeden Eintrag in der time Liste wird eine neue Zeile erstellt
df_times = df_raw.select(
    "city", "lat", "lon", explode("hourly.time").alias("hour_time")
)
# Erstellung der Indexierung um später die Zeitdaten den passenden Wetterdaten zuzuordnen
indexed_times = df_raw.select(
    "city", "lat", "lon", posexplode("hourly.time").alias("idx", "hour_time")
)

# Erstellung der Indexierung um die Temperaturwerte  zu den Zeitstempeln zuzuordnen
indexed_temp = df_raw.select(
    "city", posexplode("hourly.temperature_2m").alias("idx", "temperature_2m")
)

# Hier gleiches Prinzip für den Wind
indexed_wind = df_raw.select(
    "city", posexplode("hourly.windspeed_10m").alias("idx", "windspeed_10m")
)

# Und hier ebenfalls für die Luftfeuchtigkeit
indexed_humidity = df_raw.select(
    "city",
    posexplode("hourly.relative_humidity_2m").alias("idx", "relative_humidity_2m"),
)

# Join auf idx + city
weather_df = (
    indexed_times.join(indexed_temp, ["city", "idx"], "left")
    .join(indexed_wind, ["city", "idx"], "left")
    .join(indexed_humidity, ["city", "idx"], "left")
    .select(
        "city", "hour_time", "temperature_2m", "windspeed_10m", "relative_humidity_2m"
    )
)

# Aus hour_time wird Timestamp und Date erstellt und anschließend hour_time gelöscht
weather_clean = (
    weather_df.withColumn("timestamp", to_timestamp(col("hour_time")))
    .withColumn("date", to_date(col("timestamp")))
    .drop("hour_time")
)

# Datentypen werden angepasst
weather_clean = (
    weather_clean.withColumn("temperature_2m", col("temperature_2m").cast("double"))
    .withColumn("windspeed_10m", col("windspeed_10m").cast("double"))
    .withColumn("relative_humidity_2m", col("relative_humidity_2m").cast("int"))
)
# Aus Celius wird Fahrenheit und aus m/s wird km/h
weather_features = weather_clean.withColumn(
    "temp_f", col("temperature_2m") * 9 / 5 + 32
).withColumn("wind_kmh", col("windspeed_10m") * 3.6)

# Gruppierung nach city und date um anschließend die Durchschnittstageswerte zu errechen
weather_features = weather_features.groupBy("city", "date").agg(
    F.round(F.avg("temp_f"), 2).alias("avg_temp_f"),
    F.round(F.avg("wind_kmh"), 2).alias("avg_wind_kmh"),
    F.round(F.avg("relative_humidity_2m"), 2).alias("avg_humidity")
).sort("date", ascending=False)

# Umbennen der Spalte aufgrund der Sonderzeichen
weather_features = weather_features.withColumnRenamed("avg(temp_f)", "avg_temp_f") \
                                   .withColumnRenamed("avg(wind_kmh)", "avg_wind_kmh") \
                                   .withColumnRenamed("avg(relative_humidity_2m)", "avg_humidity")

# Speichern
weather_features.write.format("delta").mode("overwrite").saveAsTable("testcsvs_weather")
