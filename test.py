from cgitb import text
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from openpyxl import Workbook, load_workbook
import sys
from django.db.utils import IntegrityError
import psycopg2

good_cols = ["Csomagszám", "Felvétel dátuma", "Súly", "Utánvét", "Utánvét hivatkozás", "Ügyfél hivatkozás", "Címzett neve", "Futár költség", "Kiszállítási cím", "logisztika"]
bad_cols = []
for i in df.columns:
    if i not in good_cols:
        bad_cols.append(i)
print(bad_cols)
df.pop(bad_cols)
print(df)

string = string.replace(': ', '_')
print(string)