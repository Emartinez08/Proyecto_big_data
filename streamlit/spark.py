import streamlit as st
import pandas as pd
import requests
import json
import pymongo

# Inicializar conexión con MongoDB
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

client = init_connection()

@st.cache_data(ttl=600)
def get_data():
    db = client.pokemon
    items = db.pokedex.find()
    return list(items)

# Conexión con PostgreSQL
conn = st.connection("postgresql", type="sql")

# Cargar CSV desde GitHub
file_path = "https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/main/csv/pokedex.csv"

@st.cache_data
def load_csv():
    return pd.read_csv(file_path)

df = load_csv()

# Configuración de pestañas
tab1, tab2, tab3, tab4 = st.tabs(["📄 Pokedex CSV", "🔥 Spark Jobs", "📊 Resultados Spark", "📦 Bases de Datos"])

# 📄 Pestaña 1: Exploración del CSV con filtros
with tab1:
    st.title("📄 Pokedex CSV")
    st.write("Filtra y explora los Pokémon en el dataset.")

    # Extraer y limpiar los tipos de Pokémon
    df["type"] = df["type"].str.replace(r"[{}]", "", regex=True)
    all_types = sorted(set(t.strip() for types in df["type"].dropna() for t in types.split(",")))

    # Filtro por tipo (hasta 2)
    selected_types = st.multiselect("Selecciona hasta 2 tipos de Pokémon", all_types, max_selections=2)

    # Filtros por estadísticas
    sort_option = st.selectbox("Ordenar por:", ["Ninguno", "Mayor Ataque", "Mayor Defensa"])

    # Aplicar filtros
    filtered_df = df.copy()

    if selected_types:
        filtered_df = filtered_df[filtered_df["type"].apply(lambda t: all(stype in t for stype in selected_types))]

    if sort_option == "Mayor Ataque":
        filtered_df = filtered_df.sort_values(by="attack", ascending=False)
    elif sort_option == "Mayor Defensa":
        filtered_df = filtered_df.sort_values(by="defense", ascending=False)

    st.dataframe(filtered_df)

# 🔥 Pestaña 2: Spark Jobs
with tab2:
    st.title("🔥 Spark & Streamlit")
    st.header("Ejecutar Spark-submit Job")

    github_user  = st.text_input('Github user', value='Emartinez08')
    github_repo  = st.text_input('Github repo', value='Proyecto_big_data')
    spark_job    = st.text_input('Spark job', value='spark')
    github_token = st.text_input('Github token', value='', type='password')
    code_url     = st.text_input('Code URL', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/main/spark-submit/Poquedex.py')
    dataset_url  = st.text_input('Dataset URL', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/main/csv/pokedex.csv')

    def post_spark_job():
        url = f'https://api.github.com/repos/{github_user}/{github_repo}/dispatches'
        payload = {"event_type": spark_job, "client_payload": {"codeurl": code_url, "dataseturl": dataset_url}}
        headers = {'Authorization': f'Bearer {github_token}', 'Accept': 'application/vnd.github.v3+json', 'Content-type': 'application/json'}
        response = requests.post(url, json=payload, headers=headers)
        st.write(response)

    if st.button("POST Spark Submit"):
        post_spark_job()

# 📊 Pestaña 3: Resultados Spark
with tab3:
    st.title("📊 Resultados Spark")
    st.header("Ver resultados de Spark Job")

    url_summary = st.text_input('Summary Results URL', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/main/results/summary.json')
    if st.button("GET Summary Results"):
        st.json(requests.get(url_summary).json())

    url_data = st.text_input('Data Results URL', value='https://raw.githubusercontent.com/Emartinez08/Proyecto_big_data/main/results/data.json')
    if st.button("GET Data Results"):
        st.json(requests.get(url_data).json())

# 📦 Pestaña 4: Bases de Datos (MongoDB y PostgreSQL)
with tab4:
    st.title("📦 Bases de Datos")

    # MongoDB
    st.header("🔍 Consultar MongoDB")
    if st.button("Consultar MongoDB"):
        items = get_data()
        for item in items:
            item_data = json.loads(item["data"])
            st.write(f"{item_data['name']} : {item_data['type']}")

    # PostgreSQL
    st.header("📊 Consultar PostgreSQL")
    if st.button("Consultar PostgreSQL"):
        df_sql = conn.query('SELECT * FROM pokemon;', ttl="10m")
        st.dataframe(df_sql)
