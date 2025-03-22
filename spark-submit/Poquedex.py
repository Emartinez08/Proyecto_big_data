from pyspark.sql import SparkSession
import os
import json

if __name__ == "__main__":
    spark = SparkSession.builder.appName("pokedex").getOrCreate()

    print("Leyendo pokedex.csv ... ")
    path_pokedex = "pokedex.csv"
    df_pokedex = spark.read.csv(path_pokedex, header=True, inferSchema=True)
    df_pokedex.createOrReplaceTempView("pokedex")
    
    # Crear carpeta de resultados
    os.makedirs("results", exist_ok=True)

    # 🔹 Resumen (conteo de Pokémon por tipo)
    query_count = 'SELECT type, COUNT(*) AS count FROM pokedex GROUP BY type'
    df_count = spark.sql(query_count)
    
    summary_results = df_count.toPandas().to_dict(orient="records")  # ✅ Convertir a lista de diccionarios
    with open("results/summary.json", "w") as file:
        json.dump(summary_results, file, indent=2)

    # 🔹 SELECT * FROM pokedex
    query_select_all = 'SELECT * FROM pokedex'
    df_all_data = spark.sql(query_select_all)

    all_data_results = df_all_data.toPandas().to_dict(orient="records")  # ✅ Convertir a JSON correctamente
    with open("results/data.json", "w") as file:
        json.dump(all_data_results, file, indent=2)

    # 🔹 Pokémon fuertes (ataque > 100)
    query_strong_pokemon = "SELECT name, type, hp, attack FROM pokedex WHERE attack > 100 ORDER BY attack DESC"
    df_strong_pokemon = spark.sql(query_strong_pokemon)
    df_strong_pokemon.show(20)

    # 🔹 Pokémon rápidos (speed > 100)
    query_fast_pokemon = 'SELECT name, speed FROM pokedex WHERE speed > 100 ORDER BY speed DESC'
    df_fast_pokemon = spark.sql(query_fast_pokemon)
    df_fast_pokemon.show(20)

    # 🔹 Guardar Pokémon rápidos en otro archivo sin sobreescribir data.json
    fast_pokemon_results = df_fast_pokemon.toPandas().to_dict(orient="records")
    with open("results/fast_pokemon.json", "w") as file:
        json.dump(fast_pokemon_results, file, indent=2)
    
    spark.stop()
