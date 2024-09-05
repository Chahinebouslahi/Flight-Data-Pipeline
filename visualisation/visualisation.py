import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import sqlite3
db_path=r'C:\Users\bousl\OneDrive\Documents\Flight Data Pipeline\etl_pipeline\db\data.db'



def create_db_connection():
    connection = sqlite3.connect(db_path)  # Connexion à la base de données SQLite
    return connection

def extract_data(connection):
    query = "SELECT * FROM flights"
    df = pd.read_sql(query, con=connection)
    return df

# Utilisation d'un bloc try-finally pour garantir la fermeture de la connexion
db_connection = create_db_connection()

try:
    df = extract_data(db_connection)
    print(df.head())
finally:
    db_connection.close()
df=df.head(250)
# Configurer le style de Seaborn
sns.set(style="whitegrid")

# 1. Distribution des Prix des Billets d'Avion
plt.figure(figsize=(10, 6))
sns.histplot(df['Prix'], bins=30, kde=True, color='blue')
plt.title('Distribution des Prix des Billets d\'Avion')
plt.xlabel('Prix en Euros')
plt.ylabel('Nombre de Vols')
plt.show()

# 2. Variation des Prix par Compagnie Aérienne
plt.figure(figsize=(12, 8))
sns.boxplot(x='Compagnie', y='Prix', data=df)
plt.title('Variation des Prix par Compagnie Aérienne')
plt.xlabel('Compagnie Aérienne')
plt.ylabel('Prix en Euros')
plt.xticks(rotation=45)
plt.show()

# 3. Relation entre la Durée du Vol et le Prix
plt.figure(figsize=(12, 9))  # Augmentation de la largeur de la figure
sns.scatterplot(x='Durée', y='Prix', hue='Compagnie', data=df, palette='viridis')
plt.title('Relation entre la Durée du Vol et le Prix')
plt.xlabel('Durée du Vol (heures)')
plt.ylabel('Prix en Euros')

# Rotation des étiquettes de l'axe des x
plt.xticks(rotation=45, ha='right')

plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
plt.tight_layout()  # Assure que tout est bien ajusté
plt.show()

# répartition des vols par compagnies
plt.figure(figsize=(8, 8))
df['Compagnie'].value_counts().plot.pie(autopct='%1.1f%%', startangle=140, cmap='viridis')
plt.title('Répartition des Vols par Compagnie Aérienne')
plt.ylabel('')
plt.show()

#Prix moyen par compagnie aérienne
plt.figure(figsize=(10, 6))
avg_price_by_airline = df.groupby('Compagnie')['Prix'].mean().sort_values()
sns.barplot(x=avg_price_by_airline, y=avg_price_by_airline.index, hue=avg_price_by_airline.index, palette='viridis', legend=False)
plt.title('Prix Moyen par Compagnie Aérienne')
plt.xlabel('Prix Moyen en Euros')
plt.ylabel('Compagnie Aérienne')
plt.show()
