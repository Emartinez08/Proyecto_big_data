from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("pokedex")\
        .getOrCreate()

    print("Leyendo pokedex.csv ... ")
    path_pokedex = "pokedex.csv"
    df_pokedex = spark.read.csv(path_pokedex, header=True, inferSchema=True)
    df_pokedex.createOrReplaceTempView("pokedex")
    
    query = 'DESCRIBE pokedex'
    spark.sql(query).show(20)
    
    query = """SELECT name, type, hp, attack FROM pokedex WHERE attack > 100 ORDER BY attack DESC"""
    df_strong_pokemon = spark.sql(query)
    df_strong_pokemon.show(20)
    
    query = 'SELECT name, speed FROM pokedex WHERE speed > 100 ORDER BY speed DESC'
    df_fast_pokemon = spark.sql(query)
    df_fast_pokemon.show(20)
    
    results = df_fast_pokemon.toJSON().collect()
    df_fast_pokemon.write.mode("overwrite").json("results")
    
    with open('results/data.json', 'w') as file:
        json.dump(results, file)
    
    query = 'SELECT type, COUNT(*) FROM pokedex GROUP BY type'
    df_pokemon_by_type = spark.sql(query)
    df_pokemon_by_type.show()
    
    spark.stop()
