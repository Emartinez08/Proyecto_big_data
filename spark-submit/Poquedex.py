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
    
    # Get a summary (count of records by type)
    query_count = 'SELECT type, COUNT(*) AS count FROM pokedex GROUP BY type'
    df_count = spark.sql(query_count)
    
    # Save the count results in summary.json
    summary_results = df_count.toJSON().collect()
    #with open('results/summary.json', 'w') as file:
        #json.dump(summary_results, file)

    # SELECT * query to get all the data
    query_select_all = 'SELECT * FROM pokedex'
    df_all_data = spark.sql(query_select_all)

    # Save all the data to a JSON file
    all_data_results = df_all_data.toJSON().collect()
    df_all_data.write.mode("overwrite").json("results")

    # Write the SELECT * results to data.json
    #with open('results/data.json', 'w') as file:
        #json.dump(all_data_results, file)

    # Additional queries for strong and fast Pokémon
    query_strong_pokemon = """SELECT name, type, hp, attack FROM pokedex WHERE attack > 100 ORDER BY attack DESC"""
    df_strong_pokemon = spark.sql(query_strong_pokemon)
    df_strong_pokemon.show(20)
    
    query_fast_pokemon = 'SELECT name, speed FROM pokedex WHERE speed > 100 ORDER BY speed DESC'
    df_fast_pokemon = spark.sql(query_fast_pokemon)
    df_fast_pokemon.show(20)
    
    results = df_fast_pokemon.toJSON().collect()
    df_fast_pokemon.write.mode("overwrite").json("results")
    
    #with open('results/data.json', 'w') as file:
        #json.dump(results, file)
    
    spark.stop()
