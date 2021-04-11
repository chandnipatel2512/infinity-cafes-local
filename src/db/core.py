import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


HOST = os.environ.get("DB_HOST")
USER = os.environ.get("DB_USER")
PASSWORD = os.environ.get("DB_PASSWORD")
DB = os.environ.get("DB_NAME")
PORT = os.environ.get("DB_PORT")


def connection():
    return psycopg2.connect(
        user=USER, host=HOST, port=PORT, password=PASSWORD, dbname=DB
    )

conn = connection()

def query(conn, sql):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        result = cursor.fetchall()
        return result


def check(conn, sql, values):
    with conn.cursor() as cursor:
        cursor.execute(sql, values)
        result = cursor.fetchall()
        return result
    


def update(conn, sql, values, should_commit=True):
    with conn.cursor() as cursor:
        cursor.execute(sql, values)
        if should_commit:  # atomicity - ensure the update completes/fails entirely
            conn.commit()
