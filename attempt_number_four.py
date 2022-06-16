from distutils import errors
import encodings
from random import random
import string
import unicodedata
import pandas as pd
import psycopg2 
from sqlalchemy import column, create_engine
import time
import sys
import unidecode

start_time = time.time()

path = input("Mi a fájlhoz vezető út? ").lower()
table = ""
table_names = ["költségek", "bevételek", "orders", "számlák", "stock-report", "unas"]
for i in table_names:
    if i in path:
        table += i
        break
if len(table) == 0:
    table = input("Melyik tábla? ").lower()
p_key_column = []
if table == "költségek" or table == "bevételek":
    p_key_column = "Azonosító"
elif table == "orders":
    p_key_column = "Order Id"
elif table == "számlák":
    p_key_column = "Számla belső azonosító"
elif table == "stock_report":
    p_key_column.append("ID")
    p_key_column.append("Inventory Value (Layer)")
    p_key_column.append("Date")
elif table == "unas":
    p_key_column = "Cikkszám"
p_key_column = ", ".join(p_key_column)

#reading file
data = pd.read_excel(path)

if table == "költségek":
    data.rename(columns={"Megjegyzések.1":"Megjegyzések2"}, inplace=True)

# connecting to database
DB_HOST = "db-postgresql-fra1-91708-do-user-4907952-0.b.db.ondigitalocean.com"
DB_NAME = "defaultdb"
DB_USER = "doadmin"
DB_PASS = "AVNS_FovmirLSFDui0KIAOnu"
DB_PORT = "25060"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
cur = conn.cursor()
engine = create_engine("postgresql://doadmin:AVNS_FovmirLSFDui0KIAOnu@db-postgresql-fra1-91708-do-user-4907952-0.b.db.ondigitalocean.com:25060/defaultdb?sslmode=require")

#renaming columns
column_names = {}
chars_to_replace = {": ": "_", " ": "_", "(": "", ")": "", "%": "", ".": "", "\'":""}
for i in data.columns:
    column_names[i] = unidecode.unidecode(i)
    for l, j in chars_to_replace.items():
        column_names[i] = column_names[i].replace(l, j)
data.rename(columns=column_names, inplace = True)

#adding data to database
cur.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
# checking if table already exists
if table not in cur.fetchone():
    data.to_sql(table, engine, if_exists='append', index=False)
    cur.execute("ALTER TABLE "+ table +" ADD COLUMN id SERIAL;")
else:
    data.to_sql("temporary", engine, if_exists='append', index=False)

    #selecting rows that are in both the temporary and permanent table
    try:
        cur.execute("INSERT INTO "+table+" SELECT * FROM temporary WHERE (\""+p_key_column+"\") NOT IN (SELECT \""+p_key_column+"\" FROM "+table+");")
        conn.commit()
    except:
        cur.execute("DROP TABLE temporary;")
        conn.commit()
        print("Oops!", sys.exc_info()[0], "occurred.")
    

    #dropping temporary table
    cur.execute("DROP TABLE temporary;")
    conn.commit()

    #closing connection
cur.close()
conn.close()

print("Process finished --- %s seconds ---" % (time.time() - start_time))