import pandas as pd
import boto3
from io import StringIO
from airflow.models import Variable


def _list_s3_files(bucket_name: str, prefix: str) -> list:
    """
    Lists all files in an S3 bucket under a specified prefix.
    """
    aws_s3_secret_keys = Variable.get("AWS_S3_SECRET_KEYS", deserialize_json=True)
    s3_client = boto3.client("s3",
                             aws_access_key_id=aws_s3_secret_keys['AWS_ACCESS_KEY_ID'],
                             aws_secret_access_key=aws_s3_secret_keys['AWS_SECRET_ACCESS_KEY'],
                             )
    file_list = []

    paginator = s3_client.get_paginator("list_objects_v2")
    response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    for response in response_iterator:
        if "Contents" in response:
            for file in response["Contents"]:
                file_list.append(file["Key"])  # Extract file keys (paths)

    return file_list


def _read_content_files(bucket_name: str, list_files: list, file_type: str) -> pd.DataFrame:
    """
    Reads the content of CSV files from S3 and returns a consolidated Pandas DataFrame.
    """
    aws_s3_secret_keys = Variable.get("AWS_S3_SECRET_KEYS", deserialize_json=True)
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_s3_secret_keys['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=aws_s3_secret_keys['AWS_SECRET_ACCESS_KEY'],
    )

    df_final = pd.DataFrame()

    # Filter relevant files based on file_type
    relevant_files = [file for file in list_files if file_type in file and file.endswith(".csv")]

    for file in relevant_files:
        obj = s3_client.get_object(Bucket=bucket_name, Key=file)
        df_tmp = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
        df_final = pd.concat([df_final, df_tmp], ignore_index=True)

    return df_final


def extract_data(s3_prefix: str, file_type: str):
    """
    Extracts data from S3 and returns a consolidated DataFrame.
    """
    bucket_name = Variable.get("EASYREV_S3_BUCKET")
    list_files = _list_s3_files(bucket_name, prefix=s3_prefix)

    if not list_files:
        raise ValueError("No files found in S3 bucket!")

    df_final = _read_content_files(bucket_name, list_files, file_type=file_type)
    print(list_files)

    return df_final
