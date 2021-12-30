from challenge import *

# 2.4.0
# Creación de tablas para las base de datos
# 2.4.1
# Conexión con la base de datos postgresql, se debe de crear una base de datos llamada "challenge"
usuario = "postgres"
password = "0666"
engine = sqlalchemy.create_engine(
    'postgresql://'+usuario+':'+password+'@localhost:5433/challenge')

# 2.4.2
# Creación de tablas, si existen se reemplaza, no se agregan indices
museos_df.to_sql('museos', engine, if_exists='', index=False)
cines_df.to_sql('cines', engine, if_exists='replace', index=False)
bibliotecas_df.to_sql('bibliotecas', engine, if_exists='replace', index=False)

# 3.0.0
# Actualización de la base de datos
# 3.0.1
# Se agrega una nueva columna llamada fecha de carga
museos_df.insert(12, 'fecha de carga', datetime.today().strftime("%d-%m-%Y"))
cines_df.insert(12, 'fecha de carga', datetime.today().strftime("%d-%m-%Y"))
bibliotecas_df.insert(12, 'fecha de carga',
                      datetime.today().strftime("%d-%m-%Y"))

# 3.0.2
# Se cambian todos los valores "NaN" por "nulo"
museos_df.fillna('nulo', inplace=True)
cines_df.fillna('nulo', inplace=True)
bibliotecas_df.fillna('nulo', inplace=True)

# 3.0.3
# Se reemplazan los datos actualizados
museos_df.to_sql('museos', engine, if_exists='replace', index=False)
cines_df.to_sql('cines', engine, if_exists='replace', index=False)
bibliotecas_df.to_sql('bibliotecas', engine, if_exists='replace', index=False)
