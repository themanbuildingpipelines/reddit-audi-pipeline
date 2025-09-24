''' This script does the following:

1. It takes json data from Azure SQL table and locally available dataset in CSV format that acts as the base truth.
2. It combines the two tables, removing duplicates (basically enriching the limited json data)
3. Loads the data back to azure sql as a csv in the bronze layer

'''

import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import os

server = "audi-pipeline-sql-server.database.windows.net"
database = "audipipelineDB"
username = os.environ.get('username')
password = os.environ.get('password')
driver = "ODBC Driver 17 for SQL Server"

def get_engine():
    quoted_username = urllib.parse.quote_plus(username)
    quoted_password = urllib.parse.quote_plus(password)
    quoted_driver = urllib.parse.quote_plus(driver)
    connection_string = (
        f"mssql+pyodbc://{quoted_username}:{quoted_password}@{server}:1433/{database}"
        f"?driver={quoted_driver}&Encrypt=yes&TrustServerCertificate=no&Connection Timeout=30"
    )
    return create_engine(connection_string)

def load_data_to_sql(df, table_name, schema="dbo", if_exists="replace"):
    engine = get_engine()
    df.to_sql(table_name, con=engine, schema=schema, if_exists=if_exists, index=False)
    print(f"Data loaded into {schema}.{table_name}")

def enrich_data():
    csv_df = pd.read_csv(r"C:\Users\JIMMY OKOTH\Desktop\reddit_posts_original.csv")
    json_df = pd.read_csv(r"C:\Users\JIMMY OKOTH\Desktop\reddit_posts_json.csv")

    csv_df.columns = [c.lower().strip() for c in csv_df.columns]
    json_df.columns = [c.lower().strip() for c in json_df.columns]

    merged_df = pd.merge(csv_df, json_df, on="id", how="outer", suffixes=("_csv", "_json"))

    def pick_value(row, col):
        return row[f"{col}_json"] if pd.notnull(row.get(f"{col}_json")) else row.get(f"{col}_csv")

    final_df = pd.DataFrame()
    final_df["id"] = merged_df["id"]
    for col in [c.replace("_csv", "") for c in merged_df.columns if "_csv" in c]:
        final_df[col] = merged_df.apply(lambda r: pick_value(r, col), axis=1)

    final_df.drop_duplicates(subset=["title", "selftext"], inplace=True)
    return final_df

if __name__ == "__main__":
    df = enrich_data()
    load_data_to_sql(df, table_name="audi_rdata", schema="bronze")
