from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import io
import boto3

# Конфигурационные параметры для подключения к S3
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY", default_var='')
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY", default_var='')
S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL", default_var='')
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME", default_var='')
S3_CSV_KEY = Variable.get("S3_CSV_KEY", default_var='data.csv')  # Путь к CSV-файлу в бакете

# Словарь с настройками DAG по умолчанию
default_args = {
    'owner': 'airflow',                     # Владелец DAG
    'depends_on_past': False,               # DAG не зависит от успешного выполнения предыдущих запусков
    'start_date': datetime(2023, 1, 1),     # Дата начала выполнения DAG
    'email_on_failure': False,              # Не отправлять email при ошибке
    'email_on_retry': False,                # Не отправлять email при повторной попытке
    'retries': 1,                           # Количество повторных попыток при ошибке
    'retry_delay': timedelta(minutes=5),    # Задержка между повторными попытками
}

# Создание объекта DAG
dag = DAG(
    'csv_to_parquet_etl',                   # Идентификатор DAG
    default_args=default_args,              # Использование настроек по умолчанию
    description='A simple ETL DAG',         # Описание DAG
    schedule_interval=timedelta(days=1),    # Расписание выполнения - каждый день
    catchup=False,                          # Не выполнять пропущенные запуски
    tags=['airflow_dag'],
)

# Функция для создания ресурса S3
def create_s3_resource():
    # Проверка наличия необходимых параметров
    if not all([S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL]):
        raise ValueError("Необходимо установить S3_ACCESS_KEY, S3_SECRET_KEY и S3_ENDPOINT_URL в глобальных переменных.")

    # Создание ресурса S3
    s3 = boto3.resource(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )
    return s3

# Функция для извлечения данных
def extract_data(**kwargs):
    if not S3_BUCKET_NAME:
        raise ValueError("Необходимо установить S3_BUCKET_NAME в глобальных переменных.")

    s3 = create_s3_resource()
    print(f"[extract] Читаем CSV из S3: бакет={S3_BUCKET_NAME}, ключ={S3_CSV_KEY}")
    obj = s3.Object(S3_BUCKET_NAME, S3_CSV_KEY)  # Получение объекта из S3
    csv_buffer = io.BytesIO(obj.get()['Body'].read())  # Чтение содержимого файла
    df = pd.read_csv(csv_buffer)  # Чтение CSV из буфера
    print(f"[extract] Загружено строк: {len(df)}, столбцов: {len(df.columns)}")
    return df.to_json()  # Возврат данных в формате JSON

# Функция для преобразования данных
def transform_data(**kwargs):
    ti = kwargs['ti']  # Получение объекта TaskInstance
    json_data = ti.xcom_pull(task_ids='extract_task')  # Получение данных из предыдущей задачи
    df = pd.read_json(json_data)  # Преобразование JSON в DataFrame
    print(f"[transform] Получено строк для обработки: {len(df)}")

    # Агрегация по столбцам A, B, C для D
    aggregated = df.groupby(['A', 'B', 'C'])['D'].agg(['mean', 'sum', 'count']).reset_index()
    print(f"[transform] После агрегации строк: {len(aggregated)}")
    return aggregated.to_json()  # Возврат агрегированных данных в формате JSON

# Функция для загрузки данных
def load_data(**kwargs):
    ti = kwargs['ti']  # Получение объекта TaskInstance
    json_data = ti.xcom_pull(task_ids='transform_task')  # Получение данных из предыдущей задачи
    df = pd.read_json(json_data)  # Преобразование JSON в DataFrame
    print(f"[load] Получено строк для загрузки: {len(df)}")

    # Проверка наличия названия бакета
    if not S3_BUCKET_NAME:
        raise ValueError("Необходимо установить S3_BUCKET_NAME в глобальных переменных.")

    # Создание ресурса S3
    s3 = create_s3_resource()
    print(f"[load] Подключение к S3: {S3_ENDPOINT_URL}, бакет: {S3_BUCKET_NAME}")
    bucket = s3.Bucket(S3_BUCKET_NAME)  # Получение объекта бакета

    # Сохранение данных в формат Parquet
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    print(f"[load] Parquet-буфер сформирован, размер: {parquet_buffer.tell()} байт")

    # Загрузка файла в S3
    bucket.put_object(Key='aggregated_data.parquet', Body=parquet_buffer.getvalue())  # Загрузка файла в S3
    print("[load] Файл aggregated_data.parquet успешно загружен в S3")

# Определение задачи извлечения данных
extract_task = PythonOperator(
    task_id='extract_task',  # Идентификатор задачи
    python_callable=extract_data,  # Функция для выполнения
    dag=dag,  # Связь с DAG
)

# Определение задачи преобразования данных
transform_task = PythonOperator(
    task_id='transform_task',  # Идентификатор задачи
    python_callable=transform_data,  # Функция для выполнения
    dag=dag,  # Связь с DAG
)

# Определение задачи загрузки данных
load_task = PythonOperator(
    task_id='load_task',  # Идентификатор задачи
    python_callable=load_data,  # Функция для выполнения
    dag=dag,  # Связь с DAG
)

# Определение последовательности выполнения задач
extract_task >> transform_task >> load_task  # Задачи выполняются последовательно
