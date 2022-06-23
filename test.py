# from cgitb import text
# from sqlalchemy import create_engine
# from sqlalchemy.dialects.postgresql import insert
# from numpy import append
# import pandas as pd
# from openpyxl import Workbook, load_workbook
# import sys
# from django.db.utils import IntegrityError
import psycopg2
# from sqlalchemy import create_engine, true
# # import string
# import unidecode
# import time
# from datetime import date

DB_HOST = "db-postgresql-fra1-91708-do-user-4907952-0.b.db.ondigitalocean.com"
DB_NAME = "defaultdb"
DB_USER = "doadmin"
DB_PASS = "AVNS_FovmirLSFDui0KIAOnu"
DB_PORT = "25060"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST, port=DB_PORT)
cur = conn.cursor()

cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
""")
for table in list(cur.fetchone())[0].split(", "):
    cur.execute("SELECT string_agg(column_name, ', ') FROM INFORMATION_SCHEMA. COLUMNS WHERE TABLE_NAME = '"+table+"';")
    column_names = list(cur.fetchone())[0].split(", ")
    good_col_names = []
    for column in column_names:
        good_col_names.append(column.lower())
    print(table, good_col_names, column_names)
    for g_col in good_col_names:
        for b_col in column_names:
            if b_col != g_col:
                print("ALTER TABLE "+table+" RENAME COLUMN "+b_col+" to "+g_col+";")
                cur.execute("ALTER TABLE "+table+" RENAME COLUMN "+b_col+" to "+g_col+";")
    
    print(column_names)