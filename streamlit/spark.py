import streamlit as st
import requests
import pandas as pd
import json

import pymongo
# Initialize connection. Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])
client = init_connection()

# Pull data from the collection. Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def get_data():
    db = client.pokedex
    items = db.pokemon.find()
    items = list(items)  # make hashable for st.cache_data
    return items

# Initialize connection.
conn = st.connection("postgresql", type="sql")

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    payload = {
      "event_type": job,
      "client_payload": {
      "codeurl": codeurl,
      "dataseturl": dataseturl
      }
    }
    headers = {
      'Authorization': f'Bearer {token}',
      'Accept': 'application/vnd.github.v3+json',
      'Content-type': 'application/json'
    }
    st.write(url)
    st.write(payload)
    st.write(headers)
    response = requests.post(url, json=payload, headers=headers)
    st.write(response)

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)
    if response.status_code == 200:
        st.write(response.json())

st.title("Spark & Streamlit")

st.header("Spark-submit Job")

github_user  = st.text_input('Github user', value='Emartinez08')
github_repo  = st.text_input('Github repo', value='Proyecto_big_data')
spark_job    = st.text_input('Spark job', value='spark')
github_token = st.text_input('Github token', value='', type='password')
code_url     = st.text_input('Code URL', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/refs/heads/main/spark-submit/Poquedex.py')
dataset_url  = st.text_input('Dataset URL', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/refs/heads/main/csv/pokedex.csv')

if st.button("POST Spark Submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)



st.header("Query MongoDB Collection")

if st.button("Query MongoDB Collection"):
    items = get_data()
    for item in items:
        item_data = json.loads(item["data"])
        st.write(f"{item_data['name']} : {item_data['type']}")

st.header("Query PostgreSQL Table")

if st.button("Query PostgreSQL Table"):
    df = conn.query('SELECT * FROM pokedex;', ttl="10m")
    for row in df.itertuples():
        st.write(row)

st.header("Summary and Data Results")

# Provide URL for the summary and data results (files must exist in your repo or be accessible)
url_summary = st.text_input('Summary Results URL', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/main/results/summary.json')

if st.button("GET Summary Results"):
    get_spark_results(url_summary)

url_data = st.text_input('Data Results URL', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/main/results/data.json')

if st.button("GET Data Results"):
    get_spark_results(url_data)

url_fast_pokemon = st.text_input('URL results', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/main/fast_pokemon.json')

if st.button("GET Fast_pokemon Results"):
    get_spark_results(url_fast_pokemon)