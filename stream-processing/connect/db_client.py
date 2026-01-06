from typing import Any, Dict, List

import psycopg2
from psycopg2.extras import execute_values


class DBClient:
    def __init__(self, db_host: str, db_port: int, db_name: str, db_user: str, db_password: str):
        self.db_password = db_password
        self.db_user = db_user
        self.db_name = db_name
        self.db_port = db_port
        self.db_host = db_host
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                dbname=self.db_name,
                user=self.db_user,
                password=self.db_password,
            )
        except Exception as e:
            print(f"Failed to connect to PostgreSQL: {e}")
            self.conn = None

    def release_connection(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def upsert_data(
        self, table: str, data: List[Dict[str, Any]], conflict_fields: List[str], update_fields: List[str]
    ):
        """
        Upsert data into the given table.
        data: list of dicts, each dict is a row
        conflict_fields: list of columns to check for conflict (primary/unique key)
        update_fields: list of columns to update on conflict
        """
        if not self.conn:
            self.connect()
        if not self.conn:
            print("No connection available.")
            return
        if not data:
            return
        columns = data[0].keys()
        values = [[row[col] for col in columns] for row in data]
        insert_cols = ", ".join(columns)
        conflict_cols = ", ".join(conflict_fields)
        update_stmt = ", ".join([f"{col}=EXCLUDED.{col}" for col in update_fields])
        sql = f"""
            INSERT INTO {table} ({insert_cols})
            VALUES %s
            ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_stmt}
        """
        try:
            with self.conn.cursor() as cur:
                execute_values(cur, sql, values)
                self.conn.commit()
        except Exception as e:
            print(f"Upsert failed: {e}")
            self.conn.rollback()
