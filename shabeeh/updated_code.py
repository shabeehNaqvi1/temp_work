import psycopg2
from google.cloud import storage
from google.oauth2 import service_account
from psycopg2 import sql
from psycopg2.extras import execute_values
from io import BytesIO
import pandas as pd
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
    
    def create_database_if_not_exists(self, conn, db_name):
        """Ensure the database exists, create it if not."""
        # Enable autocommit for the CREATE DATABASE command to avoid transaction block issue
        conn.autocommit = True
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                if not cursor.fetchone():
                    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                    print(f"Database '{db_name}' created successfully.")
                else:
                    print(f"Database '{db_name}' already exists.")
        finally:
            # Disable autocommit after creating the database
            conn.autocommit = False

    def list_files_in_bucket_structure(self):
        structure = {"csv": {}, "images": {}}
        blobs = self.bucket.list_blobs()
        for blob in blobs:
            parts = blob.name.split('/')
            
            # Skip unexpected files
            if len(parts) < 5 or not (blob.name.endswith('.csv') or blob.name.split('.')[-1].lower() in ["jpeg", "jpg", "png", "gif", "bmp", "tiff", "webp", "svg", "heic"]):
                print(f"Skipping file with unexpected path structure: {blob.name}")
                continue

            db_name, schema_name, table_name, file_name = parts[1], parts[2], parts[3], parts[4]
            if blob.name.endswith('.csv'):
                structure["csv"].setdefault((db_name, schema_name, table_name), []).append(blob.name)
            else:
                structure["images"].setdefault((db_name, schema_name, table_name), []).append((file_name, blob.public_url))
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
    
    def create_table_from_df(self, conn, df, table_name, schema):
        with conn.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            columns = []
            for col in df.columns:
                dtype = "INTEGER" if df[col].dtype == 'int64' else "REAL" if df[col].dtype == 'float64' else "TEXT"
                columns.append(f'"{col}" {dtype}')
            columns_str = ", ".join(columns)
            cursor.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({columns_str})')
            conn.commit()

    def insert_data_into_table(self, conn, df, table_name, schema):
        columns = ', '.join(f'"{col}"' for col in df.columns)
        insert_query = f'INSERT INTO "{schema}"."{table_name}" ({columns}) VALUES %s ON CONFLICT DO NOTHING'
        
        with conn.cursor() as cursor:
            # This time we pass a list of tuples as required by execute_values
            execute_values(cursor, insert_query, df.values)
            conn.commit()

    def insert_image_metadata(self, conn, schema, table_name, image_data):
        with conn.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" (
                    id SERIAL PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE
                )
            ''')
            insert_query = f'INSERT INTO "{schema}"."{table_name}" (file_name, url) VALUES %s ON CONFLICT DO NOTHING'
            execute_values(cursor, insert_query, image_data)
            conn.commit()

    def process_database_structure(self, structure):
        connections = {}  # This will hold open connections per database
        try:
            for (db_name, schema_name, table_name), file_paths in structure["csv"].items():
                # If connection doesn't exist for the database, create it
                if db_name not in connections:
                    conn = psycopg2.connect(
                        dbname="postgres",  # Connect to the default 'postgres' database to create the target database
                        user=self.db_user,
                        password=self.db_password,
                        host=self.db_host,
                        port=self.db_port
                    )
                    self.create_database_if_not_exists(conn, db_name)
                    conn.close()  # Close the connection to the default db
                    
                    # Now, connect to the created database
                    conn = psycopg2.connect(
                        dbname=db_name,
                        user=self.db_user,
                        password=self.db_password,
                        host=self.db_host,
                        port=self.db_port
                    )
                    connections[db_name] = conn
                else:
                    conn = connections[db_name]

                # Process and insert CSV data
                merged_df = self.read_and_merge_csv_files(file_paths)
                self.create_table_from_df(conn, merged_df, table_name, schema_name)
                self.insert_data_into_table(conn, merged_df, table_name, schema_name)
                print(f"Data inserted into {db_name}.{schema_name}.{table_name}")

            # Handle image data
            for (db_name, schema_name, table_name), image_data in structure["images"].items():
                # Ensure the database is created before connecting
                if db_name not in connections:
                    conn = psycopg2.connect(
                        dbname="postgres",  # Connect to the default 'postgres' database to create the target database
                        user=self.db_user,
                        password=self.db_password,
                        host=self.db_host,
                        port=self.db_port
                    )
                    self.create_database_if_not_exists(conn, db_name)
                    conn.close()  # Close the connection to the default db
                    
                    # Now, connect to the created database
                    conn = psycopg2.connect(
                        dbname=db_name,
                        user=self.db_user,
                        password=self.db_password,
                        host=self.db_host,
                        port=self.db_port
                    )
                    connections[db_name] = conn
                else:
                    conn = connections[db_name]
                
                # Insert image metadata
                self.insert_image_metadata(conn, schema_name, table_name, image_data)
                print(f"Image metadata inserted into {db_name}.{schema_name}.{table_name}")
        finally:
            # Close all open connections at the end
            for conn in connections.values():
                conn.close()

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
