import json
import io
import boto3
import click
import pandas as pd


CONFIG = json.load(open("variables.json", "r"))

S3_ACCESS_KEY = CONFIG.get("S3_ACCESS_KEY", '')
S3_SECRET_KEY = CONFIG.get("S3_SECRET_KEY", '')
S3_ENDPOINT_URL = CONFIG.get("S3_ENDPOINT_URL", '')
S3_BUCKET_NAME = CONFIG.get("S3_BUCKET_NAME", '')


def read_parquet_from_s3(key: str = "output_data.parquet") -> pd.DataFrame:
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )
    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))
    return df


@click.command()
@click.argument("key", default="output_data.parquet")
def main(key: str):
    df = read_parquet_from_s3(key=key)
    print(df.head())
    print(f"Строк: {len(df)}, Колонок: {len(df.columns)}")


if __name__ == "__main__":
    main()
