import streamlit as st
import pandas as pd

def main():
    st.title("Pokedex CSV con Streamlit")

    # Cargar CSV desde una ruta específica
    file_path = "/home/kike/Documents/Proyecto_big_data/csv/pokedex.csv"

    try:
        df = pd.read_csv(file_path)
        st.write("Vista previa del archivo:")
        st.dataframe(df)
    except FileNotFoundError:
        st.error(f"No se encontró el archivo en la ruta: {file_path}")

if __name__ == "__main__":
    main()












