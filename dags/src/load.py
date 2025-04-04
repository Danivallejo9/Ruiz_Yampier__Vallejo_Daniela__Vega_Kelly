from typing import Dict
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import TaskInstance


def load(database: str, ti: TaskInstance):
    """Carga los DataFrames en la base de datos SQLite.

    Args:
        database (str): Ruta de la base de datos SQLite.
        ti (TaskInstance): Instancia de Airflow para obtener XCom.
    """

    # Extraemos los datos de XCom (devuelve un diccionario JSON serializado)
    extracted_data = ti.xcom_pull(task_ids='extract_data')
    
    if not extracted_data:
        raise ValueError("No se encontraron datos en XCom. Verifica 'extract_data'.")

    # Convertimos el diccionario JSON a DataFrames de pandas
    data_frames = {table: pd.DataFrame(data) for table, data in extracted_data.items()}

    # Conectamos con SQLite
    engine = create_engine(database)

    # Guardamos cada DataFrame en la base de datos
    for table_name, df in data_frames.items():
        df.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
        print(f"Tabla '{table_name}' cargada con éxito.")

    print("Carga de datos finalizada.")


