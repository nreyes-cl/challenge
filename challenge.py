import requests
import os
import locale
import pandas as pd
import matplotlib.pyplot as plt
import sqlalchemy
from datetime import datetime


# Primera parte
# 1.0.0
# Obtener y organizar archivos fuentes utilizando request
locale.setlocale(locale.LC_ALL, 'es-Es')

url_museos = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos.csv"
url_cines = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv"
url_bibliotecas = "https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv"

museos = requests.get(url_museos)
cines = requests.get(url_cines)
bibliotecas = requests.get(url_bibliotecas)

os.makedirs("museos/" + datetime.today().strftime("%Y-%B"), exist_ok=True)
os.makedirs("cines/" + datetime.today().strftime("%Y-%B"), exist_ok=True)
os.makedirs("biblioteca_popular/" +
            datetime.today().strftime("%Y-%B"), exist_ok=True)

open("C:/Users/Nicolás/Desktop/Auto Aprendizaje/Challenge/" + "museos/" + datetime.today().strftime("%Y-%B") +
     "/museos" + datetime.today().strftime("-%d-%m-%Y") + ".csv", "wb").write(museos.content)
open("C:/Users/Nicolás/Desktop/Auto Aprendizaje/Challenge/" + "cines/" + datetime.today().strftime("%Y-%B") +
     "/cines" + datetime.today().strftime("-%d-%m-%Y") + ".csv", "wb").write(cines.content)
open("C:/Users/Nicolás/Desktop/Auto Aprendizaje/Challenge/" + "biblioteca_popular/" + datetime.today().strftime("%Y-%B") +
     "/biblioteca_popular" + datetime.today().strftime("-%d-%m-%Y") + ".csv", "wb").write(bibliotecas.content)

# Segunda parte
# 2.1.0
# Procesamiento de datos y normalización de datos
museos_df = pd.read_csv("C:/Users/Nicolás/Desktop/Auto Aprendizaje/Challenge/" + "museos/" + datetime.today().strftime("%Y-%B") +
                        "/museos" + datetime.today().strftime("-%d-%m-%Y") + ".csv", keep_default_na=False, na_values=[""], encoding='latin-1', sep=',')
museos_df.drop(["observaciones", "codigo_indicativo_telefono", "latitud", "longitud", "fuente", "juridisccion",
               "anio_de_creacion", "descripcion_de_patrimonio", "anio_de_inauguracion"], axis=1, inplace=True)
museos_df = museos_df.rename(columns={"localidad_id": "cod_localidad", "provincia_id": "id_provincia", "espacio_cultural_id": "id_departamento",
                             "direccion": "domicilio", "codigo_postal": "código postal", "telefono": "número de teléfono"})
museos_df = museos_df[["cod_localidad", "id_provincia", "id_departamento", "provincia",
                       "localidad", "nombre", "domicilio", "código postal", "número de teléfono", "mail", "web"]]
museos_df.insert(3, "categoría", "Museo")

cines_df = pd.read_csv("C:/Users/Nicolás/Desktop/Auto Aprendizaje/Challenge/" + "cines/" + datetime.today().strftime("%Y-%B") +
                       "/cines" + datetime.today().strftime("-%d-%m-%Y") + ".csv", keep_default_na=False, na_values=[""], encoding='latin-1', sep=',')
cines_df.drop(["Observaciones", "CategorÃ­a", "Departamento", "Piso", "cod_area", "InformaciÃ³n adicional", "Latitud", "Longitud",
              "TipoLatitudLongitud", "Fuente", "Pantallas", "Butacas", "espacio_INCAA", "aÃ±o_actualizacion", "tipo_gestion"], axis=1, inplace=True)
cines_df = cines_df.rename(columns={"Cod_Loc": "cod_localidad", "IdProvincia": "id_provincia", "IdDepartamento": "id_departamento", "Provincia": "provincia",
                           "Localidad": "localidad", "Nombre": "nombre", "DirecciÃ³n": "domicilio", "CP": "código postal", "TelÃ©fono": "número de teléfono", "Mail": "mail", "Web": "web"})
cines_df.insert(3, "categoría", "Cine")

bibliotecas_df = pd.read_csv("C:/Users/Nicolás/Desktop/Auto Aprendizaje/Challenge/" + "biblioteca_popular/" + datetime.today().strftime(
    "%Y-%B") + "/biblioteca_popular" + datetime.today().strftime("-%d-%m-%Y") + ".csv", keep_default_na=False, na_values=[""], encoding='latin-1', sep=',')
bibliotecas_df.drop(["Observacion", "CategorÃ­a", "Subcategoria", "Departamento", "Piso", "Cod_tel", "InformaciÃ³n adicional",
                    "Latitud", "Longitud", "TipoLatitudLongitud", "Fuente", "Tipo_gestion", "aÃ±o_inicio", "AÃ±o_actualizacion"], axis=1, inplace=True)
bibliotecas_df = bibliotecas_df.rename(columns={"Cod_Loc": "cod_localidad", "IdProvincia": "id_provincia", "IdDepartamento": "id_departamento", "Provincia": "provincia",
                                       "Localidad": "localidad", "Nombre": "nombre", "Domicilio": "domicilio", "CP": "código postal", "TelÃ©fono": "número de teléfono", "Mail": "mail", "Web": "web"})
bibliotecas_df.insert(3, "categoría", "Biblioteca Popular")

# 2.2.0
# Se crea una nuevo dataframe para concatenar los dataframes de museo, cine y Biblioteca
nuevo_df = pd.concat([museos_df, cines_df, bibliotecas_df], axis=0)

# 2.2.1
# Cantidad de registros totales por categoría
contador_categoria_df = nuevo_df.groupby("categoría")["categoría"].count()
contador_categoria_df.plot(kind="bar")
plt.show()

# 2.2.2
# Cantidad de registros totales por fuente
contador_fuente_df = nuevo_df.groupby("provincia")["provincia"].count()
contador_fuente_df.plot(kind="bar")
plt.show()

# 2.2.3
# Cantidad de registros por provincia y categoría
contador_provincia_df = nuevo_df.groupby(["provincia", "categoría"])[
    "provincia"].count()
contador_provincia_df.plot(kind="bar")
plt.show()

# 2.3.0
# Procesar datos de cines
cines_tabla_df = pd.read_csv("C:/Users/Nicolás/Desktop/Auto Aprendizaje/Challenge/" + "cines/" + datetime.today().strftime(
    "%Y-%B") + "/cines" + datetime.today().strftime("-%d-%m-%Y") + ".csv", keep_default_na=False, na_values=[""], encoding='latin-1', sep=',')

# 2.3.1
# Tabla con provincias
provincia_cines_df = cines_tabla_df.groupby("Provincia")["Provincia"].count()
provincia_cines_df.plot(kind="bar")
plt.show()

# 2.3.2
# Cantidad de Pantallas
pantallas_df = cines_tabla_df.groupby("Provincia")["Pantallas"].sum()
pantallas_df.plot(kind="bar")
plt.show()

# 2.3.3
# Cantidad de Butacas
butacas_df = cines_tabla_df.groupby("Provincia")["Butacas"].sum()
butacas_df.plot(kind="bar")
plt.show()

# 2.3.4
# Cantidad de Espacios INCAA
espacio_df = cines_tabla_df.groupby("Provincia")["espacio_INCAA"].count()
espacio_df.plot(kind="bar")
plt.show()
