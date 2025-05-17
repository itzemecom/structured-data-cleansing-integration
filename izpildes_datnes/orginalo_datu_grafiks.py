import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
from IPython.display import display, HTML
from dotenv import load_dotenv
import os
import numpy as np

# 1. Ielādē vides mainīgos un DB konfigurācijju
load_dotenv()
PG_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)

# SQL vaicājums datu kvalitātes rezultātu apkopošanai
query = """
SELECT
    table_name,
    quality_dimension,
    ROUND(SUM(error_count) * 100.0 / NULLIF(SUM(total_count), 0), 2) AS error_percentage
FROM synthea_data_quality_2025
WHERE total_count > 0 -- Izslēdzam ierakstus, kur total_count ir 0
GROUP BY table_name, quality_dimension
ORDER BY table_name, quality_dimension;
"""

# Nolasa rezultātu datu rāmja (DataFrame) formā
df = pd.read_sql_query(query, get_pg_connection())

# Ja datu nav, izvada brīdinājumu
if df.empty:
    print("Nav atrastu datu kvalitātes problēmu rezultātu tabulā `synthea_data_quality_2025`.")
else:
    print("Dati veiksmīgi nolasīti, tiek veidota vizualizācija.")

    # Pārveido datus grupētam stabiņu grafikam
    grouped = df.groupby(['table_name', 'quality_dimension'])['error_percentage'].mean().unstack()
    
    # 1. Alternatīva - Horizontāls stabiņu grafiks ar kvalitātes sliekšņiem
    plt.figure(figsize=(10, 6))
    
    # Sakārto dimensijas pēc kļūdu vidējā īpatsvara
    sorted_dims = grouped.mean().sort_values(ascending=False).index
    grouped = grouped[sorted_dims]
    
    # Krāsu gradienta definēšana atkarībā no kļūdu līmeņa
    #Konkrēta krāsu shēma (no oranža uz sarkanu), jo to nodrošina plt.cm - matplotlib krāsu mapes (colormap) modulis
    colors = plt.cm.OrRd(np.linspace(0.3, 1, len(grouped.columns)))
    
    # Horizontāls uzkrātais stabiņu grafiks:
    # 1. 'bottom' satur pamata pozīcijas (sākumā 0)
    # 2. Katrai dimensijai (dim) zīmē stabiņu segmentu:
    #    - x: vērtības (kļūdu %)
    #    - y: tabulu nosaukumi
    #    - left: sāk pozīciju no iepriekšējā segmenta beigām
    #    - color: krāsa no OrRd paletes
    # 3. 'bottom += values' pārvieto pamatu nākamajam segmentam
    bottom = np.zeros(len(grouped.index))
    for i, dim in enumerate(grouped.columns):
        values = grouped[dim].fillna(0)
        plt.barh(grouped.index, values, left=bottom, color=colors[i], label=dim, edgecolor='white')
        bottom += values
    
    # Pievieno kvalitātes sliekšņus
    plt.axvline(x=1, color='green', linestyle='--', alpha=0.5, label='Ieteicamais slieksnis (1%)')
    plt.axvline(x=5, color='orange', linestyle='--', alpha=0.5, label='Brīdinājuma slieksnis (5%)')
    plt.axvline(x=10, color='red', linestyle='--', alpha=0.5, label='Kritiskais slieksnis (10%)')
    
    plt.title('Orģinālo datu kvalitātes novērtējums pa tabulām un kvalitātes dimensijām', pad=20, fontsize=14)
    plt.xlabel('Kļūdu īpatsvars (%)', fontsize=12)
    plt.ylabel('Datnes jeb tabulas (HDFS kontekstā)', fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(axis='x', alpha=0.3)
    plt.tight_layout()
    
    # 2. Alternatīva - Siltuma karte
    plt.figure(figsize=(10, 6))
    pivot_table = df.pivot(index='table_name', columns='quality_dimension', values='error_percentage')
    
    # Sakārto tabulas un dimensijas pēc kļūdu īpatsvara
    pivot_table = pivot_table.loc[pivot_table.mean(axis=1).sort_values(ascending=False).index,
                               pivot_table.mean().sort_values(ascending=False).index]
    
    plt.imshow(pivot_table.fillna(0), cmap='OrRd', aspect='auto')
    plt.colorbar(label='Kļūdu īpatsvars (%)')
    
    # Pievieno vērtības šūnās
    for i in range(len(pivot_table.index)):
        for j in range(len(pivot_table.columns)):
            value = pivot_table.iloc[i, j]
            if not np.isnan(value):
                plt.text(j, i, f'{value:.1f}%', ha='center', va='center', color='black' if value < 50 else 'white')
    
    plt.title('\n Orģinālo datu kvalitātes siltuma karte', pad=20, fontsize=14)
    plt.xlabel('Kvalitātes dimensijas', fontsize=12)
    plt.ylabel('Datnes jeb Tabulas (HDFS kontekstā)', fontsize=12)
    plt.xticks(range(len(pivot_table.columns)), pivot_table.columns, rotation=45, ha='right')
    plt.yticks(range(len(pivot_table.index)), pivot_table.index)
    plt.tight_layout()
    
    # Saglabā datu kvalitātes bildes konteinera līmenī, bet maģistra darbā, to neaktivizēju
    # plt.savefig('datu_kvalitates_horizontal.png', dpi=300, bbox_inches='tight')
    # plt.savefig('datu_kvalitates_siltuma_karte.png', dpi=300, bbox_inches='tight')
    # print("Grafiki saglabāti: 'datu_kvalitates_horizontal.png' un 'datu_kvalitates_siltuma_karte.png'")

    # Savienojums ar PostgreSQL datubāzi
    query = """
    SELECT id AS "Nr.", table_name AS "Datne", quality_dimension AS "DK dimensija", rule_name AS "Nosacījums", issue_description AS "Kļūdas apraksts",
    error_count AS "Kļūdu sk.", total_count AS "Kopējais rindu sk.", error_percentage AS "% kļūda", checked_at AS "Izskatīšanas datums"
    FROM synthea_data_quality_2025
    ORDER BY checked_at ASC
    """
    df = pd.read_sql_query(query, get_pg_connection())
    get_pg_connection().close()
    
    # Reprezentē tabulu interaktīvi
    display(HTML(df.to_html(index=False, escape=False)))
    print("\n")
    plt.show()
    print("\n")