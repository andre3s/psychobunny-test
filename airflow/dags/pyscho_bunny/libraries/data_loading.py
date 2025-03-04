import pandas as pd
import pymysql
from airflow.models import Variable

def db_mysql_connection(db_name: str) -> pymysql.Connection:
    """
    Set up and test SQLAlchemy connection for MySQL.
    """
    db_credentials = Variable.get("DB_CREDENTIALS", deserialize_json=True)
    if not db_credentials:
        error_message = "Missing 'DB_CREDENTIALS' in the Airflow Variables."
        print(message=error_message)
        raise Exception(error_message)

    required_keys = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_NAME"]
    for key in required_keys:
        if key not in db_credentials:
            error_message = f"'{key}' is missing in DB_CREDENTIALS."
            print(message=error_message)
            raise KeyError(error_message)

    try:
        connection = pymysql.connect(
            host=db_credentials["DB_HOST"],
            user=db_credentials["DB_USER"],
            password=db_credentials["DB_PASSWORD"],
            database=db_name
        )
    except Exception as e:
        error_message = f"Database connection failed: {str(e)}"
        print(message=error_message)
        raise Exception(error_message)

    return connection


def _load_data_to_table(df: pd.DataFrame, table_name: str, db_name: str) -> None:
    """
    Load data from a DataFrame to a database table.
    Removes existing rows matching today's date before inserting new data.
    Logs success or failure and sends a Slack message on failure.

    :param df: DataFrame containing the data to be loaded
    :param table_name: Name of the destination table in the database
    :param db_name: The name of the database
    """

    conn = db_mysql_connection(db_name=db_name)
    try:
        with conn.cursor() as cursor:
            truncate_query = f"TRUNCATE TABLE {db_name}.{table_name};"
            cursor.execute(truncate_query)
            conn.commit()
            print(f"Table {table_name} truncated successfully.")

            insert_query = f"INSERT INTO {db_name}.{table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
            for _, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))
            conn.commit()
            print(f"Data loaded successfully into table {table_name}. {len(df)} rows inserted.")
    except Exception as e:
        error_message = f"Failed to load data into table {table_name}: {str(e)}"
        print(error_message)
        raise e


def load_data(df: pd.DataFrame, table_name: str) -> None:

    _load_data_to_table(df=df, table_name=table_name, db_name="psychobunny_db")
