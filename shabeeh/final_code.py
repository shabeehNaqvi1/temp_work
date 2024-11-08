import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account
from sqlalchemy import create_engine, MetaData, Table, Column, text
from sqlalchemy.types import String, Integer, Float
from io import BytesIO
import psycopg2
import os


class GoogleStorageToPostgres:
    def __init__(self, db_user, db_password, db_host, db_port, bucket_name, cred_path):
        # PostgreSQL credentials
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        
        # Google Cloud credentials
        self.credentials = service_account.Credentials.from_service_account_file(cred_path)
        self.storage_client = storage.Client(credentials=self.credentials)
        self.bucket = self.storage_client.get_bucket(bucket_name)
    
    def create_database_if_not_exists(self, db_name):
        conn = None
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user=self.db_user,
                password=self.db_password,
                host=self.db_host,
                port=self.db_port
            )
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
                if not cursor.fetchone():
                    cursor.execute(f"CREATE DATABASE \"{db_name}\"")
                    print(f"Database '{db_name}' created successfully.")
                else:
                    print(f"Database '{db_name}' already exists.")
        finally:
            if conn:
                conn.close()
    
    def list_files_in_bucket_structure(self):
        structure = {}
        blobs = self.bucket.list_blobs()
        for blob in blobs:
            parts = blob.name.split('/')
            if not blob.name.endswith('.csv'):
                continue
            
            if len(parts) >= 4:
                db_name, schema_name, table_name, file_name = parts[1], parts[2], parts[3], parts[4]
                if db_name not in structure:
                    structure[db_name] = {}
                if schema_name not in structure[db_name]:
                    structure[db_name][schema_name] = {}
                if table_name not in structure[db_name][schema_name]:
                    structure[db_name][schema_name][table_name] = []
                structure[db_name][schema_name][table_name].append(blob.name)
        return structure
    
    def read_and_merge_csv_files(self, file_paths):
        dataframes = []
        for file_path in file_paths:
            blob = self.bucket.blob(file_path)
            data = blob.download_as_bytes()
            try:
                df = pd.read_csv(BytesIO(data), encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(BytesIO(data), encoding='ISO-8859-1')
            dataframes.append(df)
        return pd.concat(dataframes, ignore_index=True)
    
    def create_table_from_df(self, df, table_name, schema, engine):
        metadata = MetaData(schema=schema)
        columns = [
            Column(col, Integer if df[col].dtype == 'int64' else Float if df[col].dtype == 'float64' else String)
            for col in df.columns
        ]
        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)
    
    def process_database_structure(self, structure):
        for db_name, schemas in structure.items():
            self.create_database_if_not_exists(db_name)
            db_url = f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{db_name}"
            engine = create_engine(db_url)
            
            for schema_name, tables in schemas.items():
                quoted_schema_name = f'"{schema_name}"'
                for table_name, file_paths in tables.items():
                    merged_df = self.read_and_merge_csv_files(file_paths)
                    
                    with engine.connect() as conn:
                        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {quoted_schema_name}'))
                        conn.commit()
                    
                    self.create_table_from_df(merged_df, table_name, schema_name, engine)
                    merged_df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False)
                    print(f"Data inserted into {db_name}.{schema_name}.{table_name}")

    def run(self):
        structure = self.list_files_in_bucket_structure()
        self.process_database_structure(structure)



if __name__ == "__main__":
    # Retrieve environment variables
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    bucket_name = os.getenv("BUCKET_NAME")
    cred_path = os.getenv("CRED_PATH")

    # Initialize and run the GoogleStorageToPostgres class
    gcs_to_pg = GoogleStorageToPostgres(
        db_user=db_user,
        db_password=db_password,
        db_host=db_host,
        db_port=db_port,
        bucket_name=bucket_name,
        cred_path=cred_path
    )
    gcs_to_pg.run()
