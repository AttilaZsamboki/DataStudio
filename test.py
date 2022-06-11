import pandas as pd
import psycopg2
import csv


# Import CSV
file = input("Whats the path for the file? ")
headers_raw = []
with open(file, "r", encoding="utf8") as csv_file:
    csv_reader = list(csv.reader(csv_file, delimiter=','))
    headers_raw = [i.replace(" ", "") for i in csv_reader[0]]
headers = []
for i in headers_raw:
    if i not in headers:
        headers.append(i)

print(headers)

# with open(file, "r") as csv_file:
#     csv_reader = csv.reader(csv_file, delimiter=",")
#     datas = []
#     for row in csv_reader:
#         datas.append(row)
#     print(datas[0])
data = pd.read_csv (file, encoding="utf-8", header=0, names=headers, usecols=headers)
df = pd.DataFrame(data, columns=headers)
print(df)