import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
import os
from typing import Dict, List, Callable
from pandas import read_sql

class QueryResult:
    """Clase para almacenar resultados de consultas"""
    def __init__(self, query: str, result: pd.DataFrame):
        self.query = query
        self.result = result

def get_query_names() -> List[str]:
    """Devuelve la lista de nombres de consultas disponibles"""
    return [
        'delivery_date_difference',
        'global_ammount_order_status',
        'real_vs_estimated_delivered_time',
        'revenue_by_month_year',
        'revenue_per_state',
        'top_10_least_revenue_categories',
        'top_10_revenue_categories'
    ]

def read_query(query_name: str) -> str:
    """Lee un archivo SQL del directorio de queries"""
    dags_dir = os.path.dirname(os.path.dirname(__file__)) 
    queries_dir = os.path.join(dags_dir, 'queries') 
    query_path = os.path.join(queries_dir, f"{query_name}.sql")
    
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"No se encontró el archivo de consulta: {query_path}")
    
    with open(query_path, "r") as f:
        return f.read()

def get_all_query_functions() -> List[Callable[[Engine], QueryResult]]:
    """Devuelve una lista de funciones ejecutables para cada consulta"""
    query_functions = []
    
    for query_name in get_query_names():
        def make_query_func(name):
            def query_func(db: Engine) -> QueryResult:
                sql = read_query(name)
                df = read_sql(text(sql), db)
                return QueryResult(query=name, result=df)
            return query_func
        
        query_functions.append(make_query_func(query_name))
    
    return query_functions

def run_queries(database: Engine) -> Dict[str, pd.DataFrame]:
    """Ejecuta todas las consultas de transformación"""
    query_results = {}
    
    for query_func in get_all_query_functions():
        result = query_func(database)
        query_results[result.query] = result.result
    
    return query_results

def transform_data(database: str, output_folder: str) -> Dict[str, pd.DataFrame]:
    """
    Transforma los datos de la base de datos SQLite y guarda los resultados como archivos CSV.
    
    Args:
        database: URL de conexión a la base de datos (ej. 'sqlite:////opt/airflow/dags/olist.db')
        output_folder: Directorio donde se guardarán los archivos transformados
        
    Returns:
        Diccionario con los DataFrames resultantes de cada consulta
    """
    # Crear conexión a la base de datos
    engine = create_engine(database)
    
    # Ejecutar todas las consultas de transformación
    query_results = run_queries(engine)
    
    # Asegurar que el directorio de salida existe
    os.makedirs(output_folder, exist_ok=True)
    
    # Guardar cada resultado como archivo CSV
    for query_name, df in query_results.items():
        output_path = os.path.join(output_folder, f"{query_name}.csv")
        df.to_csv(output_path, index=False)
        print(f"Guardado {output_path} con {len(df)} registros")
    
    return query_results