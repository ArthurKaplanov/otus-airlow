import io
import json

import boto3
import pandas as pd

# Загрузка конфигурационных параметров из файла variables.json
CONFIG = json.load(open("variables.json", "r"))

# Конфигурационные параметры для подключения к S3
S3_ACCESS_KEY = CONFIG.get("S3_ACCESS_KEY", '')
S3_SECRET_KEY = CONFIG.get("S3_SECRET_KEY", '')
S3_ENDPOINT_URL = CONFIG.get("S3_ENDPOINT_URL", '')
S3_BUCKET_NAME = CONFIG.get("S3_BUCKET_NAME", '')

# Функция для создания ресурса S3
def create_s3_resource():
    # Проверка наличия необходимых параметров
    if not all([S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL]):
        raise ValueError(
            "Необходимо установить S3_ACCESS_KEY, S3_SECRET_KEY и S3_ENDPOINT_URL в глобальных переменных."
    )

    # Создание ресурса S3
    s3 = boto3.resource('s3',
                        endpoint_url=S3_ENDPOINT_URL,
                        aws_access_key_id=S3_ACCESS_KEY,
                        aws_secret_access_key=S3_SECRET_KEY)
    return s3

# Функция для извлечения данных
def extract_data():
    df = pd.read_csv("https://raw.githubusercontent.com/NickOsipov/small_data_examples/main/data.csv")
    return df

# Функция для трансформации данных
def transform_data(df):
    aggregated = df.groupby(['A', 'B', 'C'])['D'].agg(['mean', 'sum', 'count']).reset_index()
    return aggregated

# Функция для загрузки данных
def load_data(df):
    # Проверка наличия названия бакета
    if not S3_BUCKET_NAME:
        raise ValueError("Необходимо установить S3_BUCKET_NAME в глобальных переменных.")

    # Создание ресурса S3
    s3 = create_s3_resource()

    # Сохранение данных в формат Parquet
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    # Загрузка файла в S3
    bucket = s3.Bucket(S3_BUCKET_NAME)
    bucket.put_object(Key='output_data.parquet', Body=parquet_buffer.getvalue())

def main():
    # Этап извлечения данных
    df = extract_data()
    print("Данные успешно извлечены.")

    # Этап трансформации данных
    aggregated_df = transform_data(df)
    print("Данные успешно трансформированы.")

    # Этап загрузки данных
    load_data(aggregated_df)
    print("Данные успешно загружены в S3.")

if __name__ == "__main__":
    main()
