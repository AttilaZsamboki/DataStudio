import pandas as pd
import psycopg2
from sqlalchemy import create_engine

#getting infos
path = input("Mi a fájlhoz vezető út? ")
table = ""
table_names = ["költségek", "bevételek"]
for i in table_names:
    if i in path:
        table = i
        break
p_key_column = input("Melyik oszlop az elsődleges kulcs? ")

# reading file
data = pd.read_excel(path)
if table == "költségek":
    data.rename(columns={"Megjegyzések.1":"Megjegyzések2"}, inplace=True)

#connecting to database
DB_HOST = "db-postgresql-fra1-91708-do-user-4907952-0.b.db.ondigitalocean.com"
DB_NAME = "defaultdb"
DB_USER = "doadmin"
DB_PASS = "AVNS_FovmirLSFDui0KIAOnu"
DB_PORT = "25060"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
cur = conn.cursor()

#changing unique constraint
#current break
cur.execute("ALTER TABLE "+table+" DROP CONSTRAINT "+table+"_pkey;")
conn.commit()

#adding data to database
engine = create_engine("postgresql://doadmin:AVNS_FovmirLSFDui0KIAOnu@db-postgresql-fra1-91708-do-user-4907952-0.b.db.ondigitalocean.com:25060/defaultdb?sslmode=require")
data.to_sql(table, engine, if_exists='append')

#removing duplicate rows
# cur.execute("WITH funnel AS ( SELECT \""+p_key_column+"\", COUNT(*) FROM "+table+" GROUP BY 1 HAVING COUNT(*) > 1) DELETE FROM "+table+" WHERE "+table+".\""+p_key_column+"\" IN ( SELECT funnel.\""+p_key_column+"\" FROM funnel);")
cur.execute("DELETE FROM "+table+" a USING "+table+" b WHERE a.\""+p_key_column+"\" < b.\""+p_key_column+"\" AND a.\""+p_key_column+"\" = b.\""+p_key_column+"\";")
conn.commit()

#adding unique constraint back up
cur.execute("ALTER TABLE "+table+" ADD PRIMARY KEY (\""+p_key_column+"\");")
conn.commit()

#closing connection
cur.close()
conn.close()
