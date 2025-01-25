import mysql.connector
import pandas as pd

# Apro connessione con il database MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Chaddone00!",
    database="progetto_data_science"
)

cursor = conn.cursor()

# Funzione per popolare una tabella dal file CSV
def populate_table_from_csv(file_path, table_name, conn, cursor):
    # Carico il file CSV in un DataFrame
    df = pd.read_csv(file_path)
    
    # Creo un'istruzione SQL per l'inserimento dei dati
    cols = ",".join([str(i) for i in df.columns.tolist()])
    
    for i, row in df.iterrows():
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({', '.join(['%s'] * len(row))})"
        #print(sql)
        cursor.execute(sql, tuple(row))
    
    # Commit delle modifiche
    conn.commit()

path1 = 'DMS Project - Pollution In Seoul/archive/AirPollutionSeoul/Original Data/'
path2 = 'DMS Project - Pollution In Seoul/archive/AirPollutionSeoul/'

# Popoliamo le tabelle
print("Inseriamo i dati dentro 'measurement_item_info'")
populate_table_from_csv(path1 + 'measurement_item_info.csv', 'measurement_item_info', conn, cursor)
print("Inseriamo i dati dentro 'measurement_station_info'")
populate_table_from_csv(path1 + 'measurement_station_info.csv', 'measurement_station_info', conn, cursor)
print("Inseriamo i dati dentro 'measurement_info'")
populate_table_from_csv(path1 + 'measurement_info.csv', 'measurement_info', conn, cursor)
print("Inseriamo i dati dentro 'measurement_summary'")
populate_table_from_csv(path2 + 'measurement_summary.csv', 'measurement_summary', conn, cursor)

# Chiudo la connessione
cursor.close()
conn.close()