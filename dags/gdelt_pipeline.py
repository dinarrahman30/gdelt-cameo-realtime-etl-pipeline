from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import requests
import zipfile
import io
import pandas as pd

# kolom standart dari GDELT
def extract_and_transform(**kwargs):
    print("Memulai proses ekstraksi data dari GDELT API...")
    url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    response = requests.get(url)
    
    export_url = response.text.split('\n')[0].split(' ')[2]
    print(f"Mengunduh file dari: {export_url}")

    r = requests.get(export_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_filename = z.namelist()[0]
    
    with z.open(csv_filename) as f:
        
        df = pd.read_csv(f, sep='\t', header=None, low_memory=False)
    
    df_clean = df.iloc[:, [0, 1, 6, 7, 16, 17, 27, 30, 31, 60]].copy()
    
    df_clean.columns = ['GlobalEventID', 'Day', 'Actor1Name', 'Actor1CountryCode', 
                        'Actor2Name', 'Actor2CountryCode', 'EventBaseCode', 
                        'GoldsteinScale', 'NumMentions', 'SOURCEURL']

    df_clean['EventBaseCode'] = df_clean['EventBaseCode'].astype(str).str.zfill(2)
    df_clean['Day'] = pd.to_datetime(df_clean['Day'], format='%Y%m%d', errors='coerce').dt.date
    
    # Simpan CSV yang sudah bersih ke container
    output_path = '/opt/airflow/dags/temp_gdelt.csv'
    df_clean.to_csv(output_path, index=False, header=False)
    print(f"Data berhasil ditransformasi dan disimpan sementara di {output_path}")

    return output_path

def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='extract_transform_task')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    with open(csv_path, 'r') as f:
        cursor.copy_expert("""
            COPY gdelt.gdelt_events (globaleventid, sqldate, actor1_name, actor1_country, 
            actor2_name, actor2_country, cameo_code, goldstein_scale, num_mentions, source_url) 
            FROM STDIN WITH CSV
        """, f)
    
    conn.commit()
    cursor.close()
    conn.close()

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gdelt_events (
    globaleventid TEXT,
    sqldate DATE,
    actor1_name TEXT,
    actor1_country TEXT,
    actor2_name TEXT,
    actor2_country TEXT,
    cameo_code TEXT,
    goldstein_scale TEXT,
    num_mentions TEXT,
    source_url TEXT
);
"""

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('gdelt_cameo_pipeline',
         default_args=default_args,
         schedule_interval='*/15 * * * *', # Jalan setiap 15 menit sesuai update GDELT
         catchup=False) as dag:

    create_table = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='postgres_default',
        sql=CREATE_TABLE_SQL,
    )

    task1 = PythonOperator(
        task_id='extract_transform_task',
        python_callable=extract_and_transform,
    )

    task2 = PythonOperator(
        task_id='load_to_postgres_task',
        python_callable=load_to_postgres,
    )

    create_table >> task1 >> task2