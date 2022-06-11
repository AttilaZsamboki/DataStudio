from email import header
import pandas as pd
import psycopg2
import csv

# Import CSV
dataset = input("Which import are you doing? ")
file = input("Whats the path for the file? ")
headers_raw = []
with open(file, "r", encoding="utf8") as csv_file:
    csv_reader = list(csv.reader(csv_file, delimiter=','))
    headers_raw = [i.replace(" ", "").replace("(", "").replace(")", "").replace(".", "").replace("%", "") for i in csv_reader[0]]
headers = []
for i in headers_raw:
    if i not in headers:
        headers.append(i)
data = pd.read_csv (file, encoding="utf-8", header=0, names=headers, usecols=headers)
df = pd.DataFrame(data, columns=headers)

DB_HOST = "db-postgresql-fra1-91708-do-user-4907952-0.b.db.ondigitalocean.com"
DB_NAME = "defaultdb"
DB_USER = "doadmin"
DB_PASS = "AVNS_FovmirLSFDui0KIAOnu"
DB_PORT = "25060"

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)

cursor = conn.cursor()

# Insert DataFrame to Table
if dataset.lower() == 'bevételek':
    for row in df.itertuples():
        cursor.execute("SELECT azonosito FROM bevetelek WHERE azonosito = (%s);", (row.Azonosító,))
        if cursor.fetchone():
            continue
        else:
            cursor.execute('''
                    INSERT INTO bevetelek (azonosito, date, teljesitesi_datum, ar, penznem, atvaltasi_rata, afa, osszesen_huf, netto_ossz, afa_ertek, tipus, penztarca, kategoria, eredmeny_kategoria, tranzakcio_uzenet, tranzakcio_partner_fiok, tranzakcio_beszallito_neve, tranzakcio_tipusa)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (row.Azonosító,
                    row.Date,
                    row.Teljesítésdátuma,
                    row.Ár,
                    row.Pénznem,
                    row.Átváltásiráta,
                    row.ÁFA,
                    row.ÖsszesenHUF,
                    row.Nettóössz,
                    row.ÁFAérték,
                    row.Típus,
                    row.Pénztárca,
                    row.Kategória,
                    row.Eredménykategória,
                    row.Tranzakcióüzenet,
                    row.Tranzakcióbeszállítóneve,
                    row.Tranzakciópartnerfiók,
                    row.Tranzakciótípusa,
                    )
            )
elif dataset.lower() == 'költségek':
    for row in df.itertuples():
        cursor.execute("SELECT azonosito FROM koltsegek WHERE azonosito = (%s);", (row.Azonosító,))
        if cursor.fetchone():
            continue
        else:
            cursor.execute('''
                    INSERT INTO koltsegek (azonosito, honap, date, partner, koltsegelem, ar, penznem, atvaltasi_rata, afa, osszesen_huf, netto_ossz, afa_ertek, tipus, penztarca, koltseg_osztaly, iktatasi_allapot, szamla_sorszama, tranzakcio_uzenet, tranzakcio_partner_fiok, tranzakcio_beszallito_neve, tranzakcio_tipusa, teljesites_datuma, megjegyzesek, tranzakcio_belso_azonosito, tranzakcio_kulso_azonosito)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    ''',
                    (row.Azonosító,
                    row.Hónap,
                    row.Date,
                    row.Partner,
                    row.Költségelem,
                    row.Ár,
                    row.Pénznem,
                    row.Átváltásiráta,
                    row.ÁFA,
                    row.ÖsszesenHUF,
                    row.Nettóössz,
                    row.ÁFAérték,
                    row.Típus,
                    row.Pénztárca,
                    row.Költségosztály,
                    row.Iktatásiállapot,
                    row.Számlasorszáma,
                    row.Tranzakcióüzenet,
                    row.Tranzakciópartnerfiók,
                    row.Tranzakcióbeszállítóneve,
                    row.Tranzakciótípusa,
                    row.Teljesítésdátuma,
                    row.Megjegyzések,
                    row.Tranzakcióbelsőazonosító,
                    row.Tranzakciókülsőazonosító,
                    )
            )
conn.commit()
