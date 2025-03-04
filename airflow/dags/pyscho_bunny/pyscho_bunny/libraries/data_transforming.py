import pandas as pd
from datetime import datetime
import hashlib


def _hash_value(value: str) -> str:
    """Helper function to hash a given string value."""
    return hashlib.sha256(value.encode()).hexdigest() if pd.notna(value) else None


def _transform_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "first_name", "last_name", "company_name", "address", "city",
        "county", "state", "postal", "phone1", "phone2", "email", "web",
        "created_at", "updated_at"
    ]

    df_transformed = pd.DataFrame(columns=expected_columns)

    for col in ["first_name", "last_name", "company_name", "address", "city", "county",
                "phone1", "phone2", "email", "web"]:
        if col in df.columns:
            df_transformed[col] = df[col]

    # Columns to hash
    columns_to_hash = [
        "first_name", "last_name", "company_name", "email",
        "web", "phone1", "phone2", "address"
    ]

    # Apply hashing to the specified columns
    for col in columns_to_hash:
        if col in df_transformed.columns:
            df_transformed[col] = df_transformed[col].apply(_hash_value)

    df_transformed["state"] = df[["state", "province"]].bfill(axis=1).iloc[:, 0] if {"state", "province"} & set(df.columns) else None

    df_transformed["postal"] = df[["postal", "zip", "post"]].bfill(axis=1).iloc[:, 0] if {"postal", "zip", "post"} & set(df.columns) else None

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_transformed["created_at"] = now
    df_transformed["updated_at"] = now

    df_transformed = df_transformed.where(pd.notna(df_transformed), None)

    return df_transformed


def _transform_transaction_data(df: pd.DataFrame) -> pd.DataFrame:
    expected_columns = [
        "ordernumber", "quantityordered", "orderlinenumber", "total_amount", "orderdate", "qtr_id",
        "month_id", "year_id", "productcode", "customername", "phone", "addressline1", "addressline2",
        "city", "state", "postalcode", "country", "territory", "contactlastname", "contactfirstname", "dealsize"
    ]

    df.columns = df.columns.str.lower()

    available_columns = [col for col in expected_columns if col in df.columns]

    if not available_columns:
        raise ValueError("No matching columns found in DataFrame!")

    df_transformed = df[available_columns].copy()

    if "orderdate" in df_transformed.columns:
        df_transformed["orderdate"] = pd.to_datetime(df_transformed["orderdate"], errors="coerce")

    # Columns to hash
    columns_to_hash = [
        "customername", "phone", "addressline1", "addressline2",
        "contactlastname", "contactfirstname"
    ]

    # Apply hashing to the specified columns
    for col in columns_to_hash:
        if col in df_transformed.columns:
            df_transformed[col] = df_transformed[col].apply(_hash_value)

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_transformed["created_at"] = now
    df_transformed["updated_at"] = now

    df_transformed = df_transformed.where(pd.notna(df_transformed), None)

    return df_transformed


def _transform_fiscal_calendar_data(df: pd.DataFrame) -> pd.DataFrame:

    expected_columns = [
        'calendar_date', 'weekday_number', 'weekday_name', 'fiscal_week_of_month',
        'fiscal_week_of_year', 'fiscal_month_number', 'fiscal_month_name', 'fiscal_first_day_of_week',
        'fiscal_last_day_of_week', 'fiscal_first_day_of_month', 'fiscal_last_day_of_month',
        'fiscal_day_of_month', 'fiscal_quarter', 'fiscal_year', 'fiscal_year_week', 'fiscal_year_month',
        'fiscal_year_quarter', 'calendar_week_of_month', 'calendar_week_of_year', 'calendar_first_day_of_month',
        'calendar_last_day_of_month', 'calendar_first_day_of_year', 'calendar_last_day_of_year', 'weekend'
    ]

    df.columns = df.columns.str.lower()
    available_columns = [col for col in expected_columns if col in df.columns]

    if not available_columns:
        raise ValueError("No matching columns found in DataFrame!")

    df_transformed = df[available_columns].copy()

    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_transformed["created_at"] = now
    df_transformed["updated_at"] = now

    df_transformed = df_transformed.where(pd.notna(df_transformed), None)

    return df_transformed


def data_transform(df: pd.DataFrame, file_type: str) -> pd.DataFrame:
    if file_type == "customers":
        df_transformed = _transform_customer_data(df=df)
    elif file_type == "transactions":
        df_transformed = _transform_transaction_data(df=df)
    elif file_type == "de_dates":
        df_transformed = _transform_fiscal_calendar_data(df=df)
    else:
        raise ValueError(f"Unsupported file_type: {file_type}")

    return df_transformed
