from cgitb import text
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from openpyxl import Workbook, load_workbook
import sys
from django.db.utils import IntegrityError
import psycopg2

string = "abs: perimiter"

string = string.replace(': ', '_')
print(string)