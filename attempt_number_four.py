from distutils import errors
from random import random
from re import sub
from unicodedata import numeric
from numpy import datetime64
import pandas as pd
import psycopg2
from sqlalchemy import column, create_engine
import time
import unidecode
from datetime import date

table_names = ["költségek", "bevételek", "orders",
               "számlák", "stock_report", "unas", "gls_elszámolás"]
table = ""
choice = input("Út?(y/n) ").lower()
if choice == "y":
    path = input("Mi a fájlhoz vezető út? ").lower()
    for i in table_names:
        if i in path:
            table = i
            break
    if (table is None):
        table = input("Melyik tábla? ").lower()
else:
    table = input("Melyik tábla? ").lower()
        
    erp_tables = ["költségek", "bevételek", "orders",
                "számlák", "stock_report", "product_suppliers"]
    if table in erp_tables:
        path = r"C:/Users/GAMERPCX\Downloads/" + table.replace("_", "-") + "-" + date.today().strftime("%Y-%m-%d") + ".xlsx"
    else:
        path = input("Mi a fájlhoz vezető út? ").lower()


if table != "gls_elszámolás":
    org = input("Kié a tábla? ")
p_keys = {"Azonosito": ["bevételek", "költségek"], "Order_Id": ["orders"], "Szamla_belso_azonosito": [
    "számák"], "ID": ["stock_report"], "Cikkszam": ["unas"], "Csomagszam": ["gls_elszámolás"], "SKU": ["product_suppliers"]}
p_key_column = ""
for i, l in p_keys.items():
    for j in l:
        if j == table:
            p_key_column = i
            break

first_start_time = time.time()
start_time = time.time()

# reading file
if table != "gls_elszámolás":
    data = pd.read_excel(path)
else:
    data = pd.read_excel(path, skiprows=4)

if table == "költségek":
    data.rename(columns={"Megjegyzések.1": "Megjegyzések2"}, inplace=True)

df = pd.DataFrame(data)
print("File read in --- %s seconds ---" % (time.time() - start_time))
start_time = time.time()


# connecting to database
DB_HOST = "db-postgresql-fra1-91708-do-user-4907952-0.b.db.ondigitalocean.com"
DB_NAME = "defaultdb"
DB_USER = "doadmin"
DB_PASS = "AVNS_FovmirLSFDui0KIAOnu"
DB_PORT = "25060"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                        password=DB_PASS, host=DB_HOST, port=DB_PORT)
cur = conn.cursor()
engine = create_engine(
    "postgresql://doadmin:AVNS_FovmirLSFDui0KIAOnu@db-postgresql-fra1-91708-do-user-4907952-0.b.db.ondigitalocean.com:25060/defaultdb?sslmode=require")
print("Connected to database in --- %s seconds ---" %
      (time.time() - start_time))
start_time = time.time()

# renaming columns
column_names = {}
chars_to_replace = {": ": "_", " ": "_",
                    "(": "", ")": "", "%": "", ".": "", "\'": "", "-": "_"}
for i in df.columns:
    column_names[i] = unidecode.unidecode(i)
    for l, j in chars_to_replace.items():
        column_names[i] = column_names[i].replace(l, j)
df.rename(columns=column_names, inplace=True)
print("Renamed columns in --- %s seconds ---" % (time.time() - start_time))
start_time = time.time()

# changing int rows to int

def col_by_dtype(data_type, curr_table):
    data_type_str = ""
    for i in range(len(data_type)):
        if i != len(data_type)-1:
            data_type_str += "'" + data_type[i] + "',"
        else:
            data_type_str += "'" + data_type[i] + "'"
    cur.execute("""select string_agg(col.column_name, ', ')
    from information_schema.columns col
            join information_schema.tables tab on tab.table_schema = col.table_schema
        and tab.table_name = col.table_name
        and tab.table_type = 'BASE TABLE'
    where col.data_type in ("""+data_type_str+""")
    and col.table_name = '"""+curr_table+"""'
    group by col.table_schema;""")
    try:
        cols = list(cur.fetchone())[0].split(", ")
        return cols
    except:
        return None


for i in df.columns:
    cur.execute("SELECT string_agg(tablename, ', ') FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
    if table in list(cur.fetchone())[0].split(", "):

        numeric_cols = col_by_dtype(['decimal', 'numeric', 'real', 'double precision',
                                     'smallserial', 'serial', 'bigserial', 'money', 'bigint'], table)
        date_cols = col_by_dtype(['date', 'timestamp'], table)
        if (numeric_cols is not None) and i.lower() in numeric_cols:
            lst = []
            for x in df[i]:
                if type(x) == str and ',' in x:
                    lst.append(x.replace(',', '.'))
                else:
                    lst.append(x)
            df[i] = lst
            df[i] = df[i].astype(float)
        if (date_cols is not None) and i.lower() in date_cols:
            df[i] = df[i].astype(dtype=datetime64)
print(df.info())
print("Changing text rows to int --- %s seconds ---" %
      (time.time() - start_time))
start_time = time.time()

# adding differenatiator col
if table != "gls_elszámolás":
    df['org'] = org

# unas sub-categories
    # adding the new cols to the end of the dataframe
if table == "unas":
    df[[str(i)+"_alkategoria" for i in range(1, 6)]] = df["Kategoria"].str.split("|", expand=True)

    #deleting the main categories column
    df.drop("Kategoria", inplace=True, axis=1)

# adding p_key col prefix
no_p_key_pref = ["gls_elszámolás", "unas", "product_suppliers"]
if table not in no_p_key_pref:
    df[p_key_column] = org + '-' + df[p_key_column].astype(str)
print("Refining added data --- %s seconds ---" %
      (time.time() - start_time))
start_time = time.time()

# adding df to database
cur.execute("SELECT string_agg(tablename, ', ') FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';")
# checking if table already exists
if table not in list(cur.fetchone())[0].split(", "):
    df.to_sql(table, engine, index=False)
    cur.execute("ALTER TABLE " + table + " ADD COLUMN id SERIAL;")
    conn.commit()
    print("Added df to temporary table in --- %s seconds ---" %
          (time.time() - start_time))
    start_time = time.time()
else:
    df.to_sql("temporary", engine, if_exists='append', index=False)

#deleting in gls
    if table == "gls_elszámolás":
        cur.execute(
            "DELETE FROM gls_elszámolás WHERE gls_elszámolás.\"Cimzett_neve_\" IS NULL OR logisztika IS NULL;")
        cur.execute("""SELECT string_agg(column_name, ', ')
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_NAME = 'temporary'
                        AND column_name LIKE 'Unnamed%';""")
        weird_cols = list(cur.fetchone())[0].split(", ")
        for i in weird_cols:
            cur.execute("ALTER TABLE temporary DROP COLUMN \""+i+"\";")
            conn.commit()
    print("Deleting useless rows --- %s seconds ---" %
          (time.time() - start_time))
    start_time = time.time()

# selecting rows that are in both the temporary and permanent table
    cur.execute("INSERT INTO "+table+" SELECT * FROM temporary WHERE \"" +
                p_key_column+"\" NOT IN (SELECT \""+p_key_column+"\" FROM "+table+");")
    conn.commit()
    print("Selected rows that are in both the temporary and permanent table --- %s seconds ---" %
          (time.time() - start_time))
    start_time = time.time()

# dropping temporary table
    cur.execute("DROP TABLE temporary;")
    conn.commit()

# closing connection
cur.close()
conn.close()

print("Process finished --- %s seconds ---" % (time.time() - first_start_time))
