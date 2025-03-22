import streamlit as st

st.title('Hola mundo')
st.write('¡Hola mundo!')

#crea el header
st.header('Este es un header')



#crea un checkbox
if st.checkbox('Mostrar/ocultar'):
    st.write('¡Hola mundo!')

#crea un botón
if st.button('Presiona el botón'):
    st.write('¡Hola mundo!')

#crea un selectbox  
option = st.selectbox(
    '¿Cuál es tu color favorito?',
     ['Rojo', 'Verde','Azul'])

'Has seleccionado:', option

#crea un multiselect
options = st.multiselect(
    '¿Cuáles son tus colores favoritos?',
     ['Rojo', 'Verde','Azul'])

'Has seleccionado:', options

#crea un slider
age = st.slider('¿Cuál es tu edad?', 0, 130, 25)
'Edad:', age

#crea un input
title = st.text_input('Escribe un título', 'Escribe aquí...')
'Has escrito    :', title

#crea un textarea
txt = st.text_area('Escribe un texto', 'Escribe aquí...')
'Has escrito    :', txt

#crea un date_input
import datetime
d = st.date_input(
    "¿Cuál es tu fecha de nacimiento?",
    datetime.date(2019, 7, 6))
'Fecha de nacimiento:', d 

#haz un sidebar
st.sidebar.write('Este es un sidebar')
st.sidebar.write('¡Hola mundo!')


#crea un radio button
genre = st.radio(
    "¿Cuál es tu género?",
    ('Masculino', 'Femenino', 'Otro'))
'Género:', genre

#crea un file uploader
uploaded_file = st.file_uploader("Sube un archivo", type=["csv"])
if uploaded_file is not None:
    st.write(uploaded_file)

#crea un color picker
color = st.color_picker('Selecciona un color', '#00f900')
'Has seleccionado:', color  

#crea un progress bar
import time
my_bar = st.progress(0)
for percent_complete in range(100):
    time.sleep(0.1)
    my_bar.progress(percent_complete + 1)

#crea un spinner
with st.spinner('Espera por favor...'):
    time.sleep(5)
st.success('¡Listo!')

#crea un baloon
st.balloons()

#crea un json
st.json({'foo':'bar','fu':'ba'})

#crea un dataframe
import pandas as pd
df = pd.DataFrame({
  'first column': [1, 2, 3, 4],
  'second column': [10, 20, 30, 40]
})
st.write(df)

#crea un gráfico
import matplotlib.pyplot as plt
import numpy as np

st.write('Gráfico de línea')
arr = np.random.normal(1, 1, size=100)
plt.plot(arr)
st.pyplot()

#crea un mapa
import pandas as pd
import numpy as np
import pydeck as pdk

# 1. Create a DataFrame
DATA_URL = "https://raw.githubusercontent.com/ajduberstein/geo_datasets/master/housing.csv"
df = pd.read_csv(DATA_URL)

# 2. Create a layer
layer = pdk.Layer(
    "HexagonLayer",
    data=df,
    get_position=["lng", "lat"],
    auto_highlight=True,
    elevation_scale=50,
    pickable=True,
    extruded=True,
)

# 3. Set the viewport
view_state = pdk.ViewState(
    longitude=-122.4,
    latitude=37.7,
    zoom=11,
    min_zoom=5,
    max_zoom=15,
    pitch=40.5,
    bearing=-27.36,
)

# 4. Create a map
r = pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    layers=[layer],
    initial_view_state=view_state,
)

# 5. Render a map in the Streamlit app
st.pydeck_chart(r)














