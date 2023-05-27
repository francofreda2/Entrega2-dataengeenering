
"""
Created on Sat May 27 13:16:49 2023

@author: USER
"""

import psycopg2
import pandas as pd

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
data_base="data-engineer-database"
user="fredafranco13_coderhouse"
pwd= "uuS4769kWq"
conn = psycopg2.connect(
    host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    dbname=data_base,
    user=user,
    password=pwd,
    port='5439'
)

ruta_archivos='C:\\Users\\USER\\Documents\\'
product = pd.read_excel(ruta_archivos+'Datos bancos.xlsx')

def cargar_en_redshift(conn, table_name, dataframe):
    cursor = conn.cursor()
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    # Combine column definitions into the CREATE TABLE statement
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    cursor.execute(table_schema)
    # Insert data into the table
    for i, row in dataframe.iterrows():
        insert_query = f"INSERT INTO {table_name} VALUES {tuple(row.values)}"
        cursor.execute(insert_query)
    conn.commit()
    cursor.close()

cargar_en_redshift(conn, 'Datos_entrega2', product)
