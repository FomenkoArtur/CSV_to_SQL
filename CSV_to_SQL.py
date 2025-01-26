import os
import pandas as pd
import psycopg2
from psycopg2 import sql

def create_table(conn):
    with conn.cursor() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS disciplines (
                id SERIAL PRIMARY KEY,
                discipline VARCHAR(255) NOT NULL,
                department VARCHAR(255) NOT NULL,
                UNIQUE(discipline, department)
            )
        ''')
        conn.commit()

def insert_data(conn, discipline, department):
    with conn.cursor() as cursor:
        try:
            cursor.execute('''
                INSERT INTO disciplines (discipline, department) VALUES (%s, %s)
            ''', (discipline, department))
            conn.commit()
        except psycopg2.IntegrityError:
            conn.rollback()

def process_csv_files(directory, db_config):

    conn = psycopg2.connect(**db_config)
    create_table(conn)

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                print(f'File processing: {file_path}')

                df = pd.read_csv(file_path, encoding = 'utf-8')

                for index, row in df.iterrows():
                    discipline = row['Discipline']
                    department = row['Department']
                    insert_data(conn, discipline, department)

    conn.close()

db_config = {
    'dbname': 'test',
    'user': 'postgres',
    'password': 'A1qaz2wsx',
    'host': 'localhost',
    'port': '5432'
}

directory_path = 'C:\\Users\\A\\source\\repos\\CSV_to_SQL\\Bachelor'
process_csv_files(directory_path, db_config)

