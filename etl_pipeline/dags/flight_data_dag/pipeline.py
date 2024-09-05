from datetime import timedelta
import requests
from bs4 import BeautifulSoup
import os
import re
from datetime import datetime, timedelta
import time
import csv
from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

import pandas as pd
import sqlite3
import os
import numpy as np


dag_path = os.getcwd()


def scrap():
    url="https://www.google.com/travel/flights/search?tfs=CBwQAhojEgoyMDI0LTA5LTA0agwIAhIIL20vMDVxdGpyBwgBEgNUVU5AAUgBcAGCAQsI____________AZgBAg&tfu=EgYIABABGAA&gl=fr"
    # Envoyer une requête HTTP GET pour obtenir le contenu de la page
    page=requests.get(f"{url}")
    # Récupérer le contenu de la page web
    source=page.content
    time.sleep(4)
    # Parser le contenu avec BeautifulSoup en utilisant le parser lxml
    soup=BeautifulSoup(source,"lxml")
    # Pause pour respecter le délai d'attente
    time.sleep(10)
    # Liste pour stocker les détails des vols extraits
    details=[]
    time.sleep(5)
    # Obtenir le nombre total de vols trouvés
    all_flights=soup.find_all("li",{'class':"pIav2d"})
    time.sleep(1)
    length=len(all_flights)
    time.sleep(6)

    def to_date(src):
        # Utilisation de regex pour extraire la partie contenant le jour et le mois
        date_match = re.search(r'(\w{3}, \w{3} \d{1,2})', src)

        if date_match:
            # Extraire la chaîne de date 
            date_str = date_match.group(1)
    
            # Convertir la chaîne en un objet datetime
            date_obj = datetime.strptime(date_str, '%a, %b %d')
    
            # Ajouter l'année si nécessaire (par défaut, l'année courante est utilisée)
            date_obj = date_obj.replace(year=datetime.now().year)
    
            # Afficher la date sous un format spécifique
            return date_obj.strftime('%Y-%m-%d')  

        else:
            return None
    # Boucle à travers chaque vol pour extraire les informations pertinentes
    for i in range(length):

        
        # Extraire l'heure de départ
        depart=all_flights[i].find('span', {'aria-label': lambda x: x and 'Departure time' in x}).text.strip()
        
        # Extraire l'heure d'arrivée
        arrivee=all_flights[i].find('span', {'aria-label': lambda x: x and 'Arrival time' in x}).text.strip()
        
        # Extraire les prix
        prix=all_flights[i].find("div",{'class':'FpEdX'}).text.strip()

        # Extraire l'aéreport de départ
        aero_depart=all_flights[i].find("div",{'class':'QylvBf'},{'aria-describedby':"gEvJbfc309"}).text.strip()
        
        # Extraire la compagnie aérienne
        compagnie=all_flights[i].find("div",{'class':"sSHqwe tPgKwe ogfYpf"}).text.strip()

        # Extraire la durée du vol
        duree=all_flights[i].find("div",{'class':"gvkrdb AdWm1c tPgKwe ogfYpf"}).text.strip()

        # Extraire la date du vol
        jour=all_flights[i].find('span',{'class':'eoY5cb'}).text.strip()
        jdepart=to_date(jour)
        
        # Ajouter les détails du vol à la liste des détails
        details.append({"Compagnie":compagnie,
                        "Heure_du_départ":depart,
                        "Heure_arrivée":arrivee,
                        "Prix":prix,
                        "Durée":duree
                        ,"aéroport":aero_depart
                        ,"jour_du_vol":jdepart
                        
                        })




    keys=details[0].keys()
    time.sleep(1)
    
   
    # Vérifier si le fichier CSV existe déjà
    csv_file = os.path.join(dag_path,  'raw_data', 'data.csv')
    file_exists=os.path.isfile(csv_file)
    # Ouvrir le fichier CSV en mode ajout (append) et écrire les donné
    with open(csv_file,'a',newline='', encoding='utf-8') as file:
        dictWriter=csv.DictWriter(file,keys)
        # Si le fichier n'existe pas, écrire l'en-tête des colonnes
        if not file_exists:
            dictWriter.writeheader()
        # Écrire les lignes des détails des vols
        dictWriter.writerows(details)
        

def clean():
        
        # Le chemin doit être identique à celui utilisé dans extract_data
        csv_file = os.path.join(dag_path, 'raw_data', 'data.csv')
        data = pd.read_csv(csv_file, low_memory=False)
        df=pd.DataFrame(data)
        #Suppression des doublons  
        df.drop_duplicates(inplace=True)
        #Remplissage des valeurs manquantes
        df.fillna({
                'Prix': np.nan,
                'Compagnie': 'Inconnue',
                "Heure_du_départ": '00:00:00',
                "Heure_arrivée": '00:00:00'
        }, inplace=True)
        #Suppression des lignes avec prix manquants
        df = df.dropna(subset=['Prix'])

        # Conversion des Types de Données
        df['Prix'] = df['Prix'].str.replace('[^\d.]', '', regex=True).astype(float)

        # Normalisation des Données
        df['Compagnie'] = df['Compagnie'].str.upper()
        df = df[df['Compagnie'].str.len() <= 20]

        mean_price = df['Prix'].mean()
        std_price = df['Prix'].std()

        # Suppression des vols avec des prix trop éloignés de la moyenne
        
        df = df[(df['Prix'] > (mean_price - 3 * std_price)) &
                (df['Prix'] < (mean_price + 3 * std_price))]
        
        #Création d'une nouvelle colonne pour stocker le code de chaque compagnie aérienne
        df['Compagnie_code'] = df['Compagnie'].astype('category').cat.codes


        # Création d'une nouvelle colonne pour identifier si le vol est en week-end (samedi ou dimanche)
        df['jour_du_vol'] = pd.to_datetime(df['jour_du_vol'], errors='coerce')
        df['weekend_verif'] = df['jour_du_vol'].dt.weekday >= 5

        df.sort_values(by='Prix', inplace=True)
        df.reset_index(drop=True, inplace=True)

        df.to_csv(f"{dag_path}/clean_data/clean_data.csv", index=False)
        

def load_data():
    # Connexion à la base de données SQLite
    conn = sqlite3.connect("/usr/local/airflow/db/data.db")
    c = conn.cursor()
    # créeation de la table flights
    c.execute("""
    CREATE TABLE IF NOT EXISTS flights (
        Compagnie TEXT,
        Heure_du_départ TEXT,
        Heure_arrivée TEXT,
        Prix REAL,
        Durée TEXT,
        aéroport TEXT,
        jour_du_vol TEXT,
        Compagnie_code INTEGER,
        weekend_verif BOOLEAN
    );
    """)
    #stocker les données dans la base de données
    df = pd.read_csv(f"{dag_path}/clean_data/clean_data.csv")
    df.to_sql('flights', conn, if_exists='replace', index=False)


# initialiser les arguments par défauts
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}
#Création du dag pour le pipeline
ingestion_dag = DAG(
    'flight_etl_pipeline',
    default_args=default_args,
    description='etl pipeline for analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)
#définir les tâches 
task_1 = PythonOperator(
    task_id='extract_data',
    python_callable=scrap,
    dag=ingestion_dag,
)


task_2 = PythonOperator(
    task_id='transform_data',
    python_callable=clean,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)
#ordre des tâches
task_1 >> task_2 >> task_3
