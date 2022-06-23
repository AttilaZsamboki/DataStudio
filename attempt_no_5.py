import math
import urllib.request
from zipfile import ZipFile
import gzip
import pandas as pd
import time
import psycopg2
from sqlalchemy import create_engine, null
import sys
import unidecode

start_time = time.time()

downloads = {"https://datasets.imdbws.com/name.basics.tsv.gz":["name_basics", "nconst", "db-postgresql-fra1-67534-do-user-4907952-0.b.db.ondigitalocean.com", "defaultdb", "doadmin", "AVNS_ymRwUGDjHvDAscsnnOT", "25060"], "https://datasets.imdbws.com/title.akas.tsv.gz": ["title_akas", "titleId", "db-postgresql-fra1-67534-do-user-4907952-0.b.db.ondigitalocean.com", "defaultdb", "doadmin", "AVNS_ymRwUGDjHvDAscsnnOT", "25060"], "https://datasets.imdbws.com/title.basics.tsv.gz":["title_basics", "tconst", "db-postgresql-fra1-67534-do-user-4907952-0.b.db.ondigitalocean.com", "defaultdb", "doadmin", "AVNS_ymRwUGDjHvDAscsnnOT", "25060"], "https://datasets.imdbws.com/title.crew.tsv.gz":["title_crew", "tconst" "db-postgresql-fra1-67534-do-user-4907952-0.b.db.ondigitalocean.com", "defaultdb", "doadmin", "AVNS_ymRwUGDjHvDAscsnnOT", "25060"], "https://datasets.imdbws.com/title.principals.tsv.gz":["title_principals", "tconst", "db-postgresql-fra1-67534-do-user-4907952-0.b.db.ondigitalocean.com", "defaultdb", "doadmin", "AVNS_ymRwUGDjHvDAscsnnOT", "25060"], "https://datasets.imdbws.com/title.ratings.tsv.gz":["title_ratings", "tconst", "db-postgresql-fra1-67534-do-user-4907952-0.b.db.ondigitalocean.com", "defaultdb", "doadmin", "AVNS_ymRwUGDjHvDAscsnnOT", "25060"]}

for i,l in downloads.items():
# getting data from the NET
    data = urllib.request.urlopen(i)
    data = gzip.open(data, "rb")
    data = pd.read_csv(data, sep="\t")
    print("Getting the data from the internet finished for "+l[0]+" --- "+str(math.floor((time.time() - start_time) / 60))+" minutes and "+str((time.time() - start_time) % 60)+" seconds ---")

# connecting to DB
    DB_HOST = l[2]
    DB_NAME = l[3]
    DB_USER = l[4]
    DB_PASS = l[5]
    DB_PORT = l[6]

    table = l[0]
    p_key_column = l[1]

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
    cur = conn.cursor()
    engine = create_engine("postgresql://"+DB_USER+":"+DB_PASS+"@"+DB_HOST+":"+DB_PORT+"/"+DB_NAME+"?sslmode=require")

#renaming columns
    column_names = {}
    chars_to_replace = {": ": "_", " ": "_", "(": "", ")": "", "%": "", ".": "", "\'":""}
    for i in data.columns:
        column_names[i] = unidecode.unidecode(i)
        for l, j in chars_to_replace.items():
            column_names[i] = column_names[i].replace(l, j)
    data.rename(columns=column_names, inplace = True)
# checking if table already exists
    data.to_sql(table, engine, if_exists="replace")
    print("Process finished for "+l[0]+" --- "+str(math.floor((time.time() - start_time) / 60))+" minutes and "+str((time.time() - start_time) % 60)+" seconds ---")
