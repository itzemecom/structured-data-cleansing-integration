import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_sub, rand, when, concat, lit, date_format, sum as spark_sum, min, max, count, lower, upper, trim, regexp_replace, regexp_extract, current_date, datediff, months_between, floor, coalesce
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType, TimestampType, IntegerType, BooleanType
import json
import pandas as pd

# 1. Ielādē vides mainīgos un DB konfigurāciju
load_dotenv()
PG_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

def get_pg_connection():
    """Izveido savienojumu ar PostgreSQL datu bāzi."""
    return psycopg2.connect(**PG_CONFIG)


def record_issue(results, table, check_type, column, desc, affected, total):
    """
    Reģistrē datu kvalitātes problēmu vai labojumu.

    Args:
        results (list): Saraksts, kurā tiek glabāti visi rezultāti.
        table (str): Tabulas nosaukums, kurā tika atrasta problēma.
        check_type (str): Datu kvalitātes dimensija (piemēram, "5SV", "8BL", "9S", "10CL", "11FI", "12C", "12T", "13QI", "14V", "15C").
        column (str): Kolonnas nosaukums, kurā tika atrasta problēma (vai vispārīgs apraksts).
        desc (str): Problēmas apraksts.
        affected (int): Ietekmēto rindu skaits.
        total (int): Kopējais rindu skaits tabulā.
    """
    actual_affected = int(affected) if affected is not None else 0
    actual_total = int(total) if total is not None else 0
    percent = round((actual_affected / actual_total) * 100.0, 2) if actual_total > 0 else 0.0

    results.append({
        "table_name": table,
        "quality_dimension": check_type,
        "rule_name": column,
        "issue_description": desc,
        "error_count": actual_affected,
        "total_count": actual_total,
        "error_percentage": percent
    })
    
# 2. Spark sesija
# Svarīgi: spark.executor.cores ir iestatīts uz "1", lai atbilstu jūsu Spark darbinieku (worker) kodolu skaitam.
# Tas nodrošina, ka Spark Master var veiksmīgi piešķirt resursus aplikācijai.
spark = SparkSession.builder \
    .appName("kludaino_SyntheaVeselibasDatuKvalitatesNovertesanaHDFS") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.debug.maxToStringFields", 100) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.scheduler.listenerbus.eventqueue.capacity", "20000") \
    .config("spark.pyspark.python", "/opt/venv/bin/python") \
    .config("spark.pyspark.driver.python", "/opt/venv/bin/python") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# Nodrošinām, ka Spark sesija tiks pārtraukta neatkarīgi no kļūdām
try:
    # 3. Datu nolasīšana no HDFS
    # Šis ir ceļš uz SĀKOTNĒJIEM, klūdainajiem datiem, ko mēs labosim.
    base_path = "/dati/synthea_kludainie_dati1" 
    valid_formats = ["csv", "json", "parquet", "avro", "delta"]
    df_map = {}

    try: # Pievienots try/except ap HDFS piekļuvi
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        Path = spark._jvm.org.apache.hadoop.fs.Path

        base_hdfs_path_obj = Path(base_path)
        if not fs.exists(base_hdfs_path_obj):
            print(f"Kļūda: HDFS bāses ceļš {base_path} neeksistē.")
        elif not fs.isDirectory(base_hdfs_path_obj): # Labots mainīgā nosaukums šeit
            print(f"Kļūda: HDFS bāses ceļš {base_path} nav direktorijas.")
        else:
            for table_status in fs.listStatus(base_hdfs_path_obj):
                if table_status.isDirectory():
                    table_dir = table_status.getPath().toString()
                    table_name = os.path.basename(table_dir).replace('_corrupted','')
                    for fmt_status in fs.listStatus(Path(table_dir)):
                        fmt_dir = fmt_status.getPath().toString()
                        fmt = os.path.basename(fmt_dir).lower()
                        if fmt in valid_formats:
                            try: # Pievienots try/except ap datņu lasīšanu
                                print(f"Mēģinu ielādēt tabulu '{table_name}' no formāta '{fmt}' ceļā {fmt_dir}...")
                                df = None # Inicē df
                                if fmt == "csv":
                                    df = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(fmt_dir)
                                elif fmt == "json":
                                    df = spark.read.option("header", True).option("mode", "PERMISSIVE").json(fmt_dir)
                                elif fmt == "parquet":
                                    df = spark.read.parquet(fmt_dir)
                                elif fmt == "avro":
                                    df = spark.read.format("avro").load(fmt_dir)
                                elif fmt == "delta":
                                    df = spark.read.format("parquet").load(fmt_dir)
                                
                                # Pārbaudām, vai DataFrame ir patiešām ielādēts un lietojams
                                if df is not None:
                                    df.count() # Mazs tests, lai pārliecinātos, ka DataFrame ir derīgs
                                    df_map[table_name] = df
                                    print(f"  Veiksmīgi ielādēta tabula '{table_name}'.")
                                    break # Atrasts derīgs formāts, pāriet pie nākamās tabulas
                                else:
                                    print(f"  Brīdinājums: Neizdevās ielādēt DataFrame no {fmt_dir} ({fmt}).")

                            except Exception as read_e:
                                print(f"Kļūda lasot datus no {fmt_dir} ({fmt}): {read_e}")
                                continue # Pārejam pie nākamā formāta, ja šis neizdevās

    except Exception as hdfs_e:
        print(f"Kļūda piekļūstot HDFS bāses ceļam {base_path} vai apstrādājot apakšdirektorijas: {hdfs_e}")

    results = [] # Šis saraksts tagad glabās rezultātus visiem posmiem

    # --- 4. posms: Datu struktūras pārskats ---
    print("\n--- 4. posms: Datu struktūras pārskats ---")
    total_tables_loaded = len(df_map)
    print(f"Kopējais ielādēto tabulu skaits: {total_tables_loaded}\n")

    print("Kolonnu skaits katrā tabulā:")
    print("----------------------------")
    for name, df in df_map.items():
        num_columns = len(df.columns)
        print(f"Tabula '{name}': {num_columns} kolonnas")
    print("----------------------------\n")

    # --- Definējam paredzētās shēmas (šīs ir jāpielāgo jūsu reālajām shēmām!) ---
    expected_schemas = {
        "allergies": StructType([
            StructField("START", StringType(), True), # MAINĪTS UZ STRINGTYPE
            StructField("STOP", StringType(), True),   # MAINĪTS UZ STRINGTYPE
            StructField("PATIENT", StringType(), True),
            StructField("ENCOUNTER", StringType(), True),
            StructField("CODE", StringType(), True),
            StructField("SYSTEM", StringType(), True),
            StructField("DISPLAY", StringType(), True),
            StructField("TYPE", StringType(), True),
            StructField("CATEGORY", StringType(), True),
            StructField("CRITICALITY", StringType(), True),
            StructField("DESCRIPTION", StringType(), True),
            StructField("REACTION1", StringType(), True),
            StructField("SEVERITY1", StringType(), True),
            StructField("REACTION2", StringType(), True),
            StructField("SEVERITY2", StringType(), True),
        ]),
        "patients": StructType([
            StructField("ID", StringType(), True),
            StructField("BIRTHDATE", DateType(), True), 
            StructField("DEATHDATE", DateType(), True), 
            StructField("SSN", StringType(), True),
            StructField("DRIVERS", StringType(), True),
            StructField("PASSPORT", StringType(), True),
            StructField("PREFIX", StringType(), True),
            StructField("FIRST", StringType(), True),
            StructField("LAST", StringType(), True),
            StructField("SUFFIX", StringType(), True),
            StructField("MAIDEN", StringType(), True),
            StructField("MARITAL", StringType(), True),
            StructField("RACE", StringType(), True),
            StructField("ETHNICITY", StringType(), True),
            StructField("GENDER", StringType(), True),
            StructField("BIRTHPLACE", StringType(), True),
            StructField("ADDRESS", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE", StringType(), True),
            StructField("ZIP", StringType(), True),
        ]),
        "encounters": StructType([
            StructField("ID", StringType(), True), # Pievienots ID, jo jūsu injekcija to modificē
            StructField("START", LongType(), True),
            StructField("STOP", LongType(), True),
            StructField("PATIENT", StringType(), True),
            StructField("ORGANIZATION", StringType(), True),
            StructField("PROVIDER", StringType(), True),
            StructField("PAYER", StringType(), True),
            StructField("ENCOUNTERCLASS", StringType(), True),
            StructField("CODE", StringType(), True),
            StructField("DISPLAY", StringType(), True),
            StructField("DESCRIPTION", StringType(), True),
            StructField("BASE_ENCOUNTER", StringType(), True),
            StructField("REASONCODE", StringType(), True),
            StructField("REASONDISPLAY", StringType(), True),
        ]),
        "conditions": StructType([
            StructField("START", LongType(), True),
            StructField("STOP", LongType(), True),
            StructField("PATIENT", StringType(), True),
            StructField("ENCOUNTER", StringType(), True),
            StructField("CODE", StringType(), True),
            StructField("SYSTEM", StringType(), True),
            StructField("DISPLAY", StringType(), True),
            StructField("DESCRIPTION", StringType(), True), # Pievienots DESCRIPTION, lai varētu veikt semantisko labošanu
        ]),
        "medications": StructType([
            StructField("START", LongType(), True),
            StructField("STOP", LongType(), True),
            StructField("PATIENT", StringType(), True),
            StructField("ENCOUNTER", StringType(), True),
            StructField("CODE", StringType(), True),
            StructField("SYSTEM", StringType(), True),
            StructField("DISPLAY", StringType(), True),
            StructField("PRESCRIPTION", StringType(), True),
            StructField("REASONDISPLAY", StringType(), True),
            StructField("REASONCODE", StringType(), True),
        ]),
        # ... Pievienojiet citas tabulas, kas jums ir Synthea datos
    }

    # Definējam primārās atslēgas (PK) katrai tabulai
    # Svarīgi: Šeit mēs definējam unikālo atslēgu, lai noņemtu patiesos dublikātus.
    # Tabulām, kurām nav vienas kolonnas unikāla ID (piemēram, CODE nav unikāls katrai medikamenta devai),
    # mēs izmantojam kompozīto atslēgu.
    primary_keys = {
        "patients": ["ID"],
        "encounters": ["ID"], # Encounters ID ir unikāls katrai vizītei
        "conditions": ["PATIENT", "CODE", "START"], # Kombinācija, lai identificētu unikālu stāvokļa ierakstu
        "medications": ["PATIENT", "CODE", "START"], # Kombinācija, lai identificētu unikālu medikamentu ierakstu
        "allergies": ["PATIENT", "CODE", "START"], # Kombinācija, lai identificētu unikālu alerģijas ierakstu
    }

    # Definējam atsauces integritātes noteikumus
    reference_rules = {
        "allergies": [("PATIENT", "patients", "ID"), ("ENCOUNTER", "encounters", "ID")],
        "conditions": [("PATIENT", "patients", "ID"), ("ENCOUNTER", "encounters", "ID")],
        "encounters": [("PATIENT", "patients", "ID")],
        "medications": [("PATIENT", "patients", "ID"), ("ENCOUNTER", "encounters", "ID")],
    }

    # --- Domēnu definīcijas ---
    # Šī vārdnīca definē katra domēna atslēgvārdus, aprakstu un kolonnas, kuras nav obligāti aizpildāmas.
    # To var paplašināt ar citām domēnspecifiskām zināšanām.
    domain_definitions = {
        "Veselības aprūpe": {
            "keywords": ["patients", "allergies", "conditions", "encounters", "medications", "providers", "organizations", "immunizations", "procedures"],
            "description": "Dati, kas saistīti ar pacientiem, medicīniskajām diagnozēm, ārstēšanu, alerģijām un pakalpojumu sniedzējiem.",
            "nullable_columns": { # Jauns: Kolonnas, kuras var būt NULL, neuzskatot to par kļūdu
                "patients": ["DEATHDATE"],
                # Pievienojiet citas tabulas un to nullable kolonnas šeit
            }
        },
        "Izglītība": {
            "keywords": ["students", "courses", "enrollments", "teachers", "grades", "schools", "curriculum"],
            "description": "Dati, kas saistīti ar studentiem, mācību kursiem, atzīmēm un izglītības iestādēm.",
            "nullable_columns": {
                # Piemērs: "students": ["GRADUATION_DATE"]
            }
        },
        "Rūpniecība": {
            "keywords": ["products", "manufacturing", "inventory", "suppliers", "customers", "orders", "production_lines"],
            "description": "Dati, kas saistīti ar produktu ražošanu, krājumiem, piegādes ķēdēm un klientu pasūtījumiem.",
            "nullable_columns": {
                # Piemērs: "products": ["EXPIRATION_DATE"]
            }
        }
    }


    # --- 5. posms: Datu domēna noteikšana ---
    print("\n--- 5. posms: Datu domēna noteikšana ---")
    determined_domain = "Nenoteikts" # Noklusējuma vērtība
    
    # Mēģinām noteikt domēnu, pamatojoties uz ielādēto tabulu nosaukumiem
    for domain_name, domain_info in domain_definitions.items():
        if any(keyword in df_map for keyword in domain_info["keywords"]):
            determined_domain = domain_name
            print(f"Datu domēns noteikts kā: '{determined_domain}' (pamatojoties uz atslēgvārdiem: {', '.join(domain_info['keywords'])}).")
            record_issue(results, "Global", "5DD", "Domain Detection", f"Datu domēns noteikts kā '{determined_domain}'.", 0, 1)
            break # Atrasts domēns, pārtraucam meklēšanu
    
    if determined_domain == "Nenoteikts":
        print(f"Datu domēns nav noteikts. Turpmākās domēnspecifiskās pārbaudes var tikt izlaistas.")
        record_issue(results, "Global", "5DD", "Domain Detection", "Datu domēns nav noteikts.", 1, 1)


    # --- 6. posms: Metadatu un shēmas validācija ---
    print("\n--- 6. posms: Metadatu un shēmas validācija ---")

    for name, df in df_map.items():
        print(f"\nVeicu shēmas validāciju tabulai: '{name}'")
        
        if name not in expected_schemas:
            print(f"  Brīdinājums: Paredzētā shēma tabulai '{name}' nav definēta. Tiek izlaista shēmas validācija.")
            record_issue(results, name, "5MD", "Schema Definition", "Paredzētā shēma nav definēta", 1, 1)
            continue 

        actual_schema = df.schema
        expected_schema = expected_schemas[name]

        for expected_field in expected_schema.fields:
            column_name = expected_field.name
            expected_type = expected_field.dataType

            if column_name not in df.columns:
                issue_desc = f"Trūkst paredzētās kolonnas '{column_name}'"
                record_issue(results, name, "5SV", column_name, issue_desc, 1, len(expected_schema.fields))
                print(f"  {issue_desc}")
            else:
                actual_type = df.schema[column_name].dataType
                if actual_type != expected_type:
                    issue_desc = f"Kolonnai '{column_name}' neatbilst datu tips: Paredzēts '{expected_type}', atrasts '{actual_type}'"
                    record_issue(results, name, "5SV", column_name, issue_desc, 1, len(expected_schema.fields))
                    print(f"  {issue_desc}")
        
        actual_column_names = set(df.columns)
        expected_column_names = set([f.name for f in expected_schema.fields])
        
        unexpected_columns = actual_column_names - expected_column_names
        if unexpected_columns:
            issue_desc = f"Atrastas negaidītas kolonnas: {', '.join(unexpected_columns)}"
            record_issue(results, name, "5SV", "Unexpected Columns", issue_desc, len(unexpected_columns), len(actual_column_names))
            print(f"  {issue_desc}")
            

    # --- 7. Sākam datu kvalitātes pārbaudes (pamatcilps) ---
    print("\n--- 7. Sākam datu kvalitātes pārbaudes ---")
    
    for name, df in df_map.items():
        print(f"\nApstrādājam tabulu: '{name}'")

        df.cache() # Kešojam DataFrame pirms dārgām darbībām.
        
        try:
            total_rows = df.count() 
        except Exception as e:
            print(f"  Kļūda, veicot count() tabulai '{name}': {e}. Izlaižam šo tabulu.")
            record_issue(results, name, "6V", "Volume", "Error counting rows", 0, 0)
            df.unpersist()
            continue

        if total_rows == 0:
            record_issue(results, name, "6V", "Volume", "Total rows", 0, 0)
            print(f"  Brīdinājums: Tabula '{name}' ir tukša (0 rindas). Tika reģistrēta apjoma problēma. Pārējās pārbaudes izlaistas.")
            df.unpersist()
            continue

        print(f"  Tabula '{name}' satur {total_rows} rindas un {len(df.columns)} kolonnas.")
        
        # --- 8. posms: Statiskā/sintaktiskā validācija (formāti, tipi, diapazoni) ---
        print(f"\n--- 8. posms: Statiskā/sintaktiskā validācija tabulai: '{name}' ---")

        # Pārbaude: NULL vērtības kritiskās kolonnās (vispārīgi)
        for col_name in df.columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                record_issue(results, name, "7S", col_name, f"Trūkstošas vērtības (NULL)", null_count, total_rows)
            else: # Ja nav NULL vērtību
                record_issue(results, name, "7S", col_name, f"Trūkstošas vērtības (NULL) - OK", 0, total_rows)


        # Specifiskas pārbaudes, balstoties uz kļūdu injekcijas paraugu
        if determined_domain == "Veselības aprūpe": # Izpilda tikai veselības aprūpes domēnam
            if name == "patients":
                # Pārbaude: Dzimšanas datums ir pārāk sens (19.gs.)
                ancient_birthdate_count = df.filter(
                    (col("BIRTHDATE").isNotNull()) & 
                    (col("BIRTHDATE").cast(StringType()).startswith("19"))
                ).count()
                if ancient_birthdate_count > 0:
                    record_issue(results, name, "7S", "BIRTHDATE", "Dzimšanas datums ir pārāk sens (19.gs.)", ancient_birthdate_count, total_rows)
                    print(f"  Tabulā '{name}': {ancient_birthdate_count} pacienti ar dzimšanas datumu 19. gadsimtā.")
                else:
                    record_issue(results, name, "7S", "BIRTHDATE", "Dzimšanas datums ir pārāk sens (19.gs.) - OK", 0, total_rows)

                # Pārbaude: Nāves datums pirms dzimšanas datuma
                death_before_birth_count = df.filter(
                    (col("BIRTHDATE").isNotNull()) & 
                    (col("DEATHDATE").isNotNull()) & 
                    (to_date(col("DEATHDATE")).isNotNull()) & 
                    (to_date(col("BIRTHDATE")).isNotNull()) & 
                    (to_date(col("DEATHDATE")) < to_date(col("BIRTHDATE")))
                ).count()
                if death_before_birth_count > 0:
                    record_issue(results, name, "7S", "DEATHDATE", "Nāves datums pirms dzimšanas datura", death_before_birth_count, total_rows)
                    print(f"  Tabulā '{name}': {death_before_birth_count} pacienti ar nāves datumu pirms dzimšanas datura.")
                else:
                    record_issue(results, name, "7S", "DEATHDATE", "Nāves datums pirms dzimšanas datura - OK", 0, total_rows)

                # Pārbaude: Nederīga dzimuma vērtība ('X' vai NULL)
                invalid_gender_count = df.filter(
                    (col("GENDER").isNull()) | 
                    (col("GENDER") == "X") | 
                    (~lower(col("GENDER")).isin("m", "f"))
                ).count()
                if invalid_gender_count > 0:
                    record_issue(results, name, "7S", "GENDER", "Nederīga dzimuma vērtība (NULL vai 'X')", invalid_gender_count, total_rows)
                    print(f"  Tabulā '{name}': {invalid_gender_count} pacienti ar nederīgu dzimuma vērtību.")
                else:
                    record_issue(results, name, "7S", "GENDER", "Nederīga dzimuma vērtība - OK", 0, total_rows)

                # Jauns: Pārbaude, vai dzimšanas datums ir NULL
                null_birthdate_count = df.filter(col("BIRTHDATE").isNull()).count()
                if null_birthdate_count > 0:
                    record_issue(results, name, "7S", "BIRTHDATE_Completeness", "Dzimšanas datums ir NULL", null_birthdate_count, total_rows)
                    print(f"  Tabulā '{name}': {null_birthdate_count} pacienti ar NULL dzimšanas datumu.")
                else:
                    record_issue(results, name, "7S", "BIRTHDATE_Completeness", "Dzimšanas datums ir NULL - OK", 0, total_rows)

                # Jauns: Pārbaude, vai dzimšanas datums ir nederīgā formātā (ja tas ir virknes tips)
                if "BIRTHDATE" in df.columns and df.schema["BIRTHDATE"].dataType == StringType():
                    malformed_birthdate_count = df.filter(
                        (col("BIRTHDATE").isNotNull()) &
                        (to_date(col("BIRTHDATE"), "yyyy-MM-dd").isNull()) &
                        (to_date(col("BIRTHDATE"), "MM/dd/yyyy").isNull()) & # Pievienoti bieži sastopami formāti
                        (to_date(col("BIRTHDATE"), "yyyy.MM.dd").isNull())
                    ).count()
                    if malformed_birthdate_count > 0:
                        record_issue(results, name, "7S", "BIRTHDATE_Format", "Dzimšanas datums ir nederīgā formātā", malformed_birthdate_count, total_rows)
                        print(f"  Tabulā '{name}': {malformed_birthdate_count} pacienti ar nederīgu dzimšanas datuma formātu.")
                    else:
                        record_issue(results, name, "7S", "BIRTHDATE_Format", "Dzimšanas datums ir nederīgā formātā - OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "BIRTHDATE_Format", "Dzimšanas datuma formāta pārbaude netika veikta (nav StringType vai trūkst kolonnas)", 0, total_rows)

                # JAUNS: Pārbaude, vai dzimšanas datums nav nākotnē
                if "BIRTHDATE" in df.columns and df.schema["BIRTHDATE"].dataType == DateType():
                    future_birthdate_count = df.filter(
                        (col("BIRTHDATE").isNotNull()) & 
                        (col("BIRTHDATE") > current_date())
                    ).count()
                    if future_birthdate_count > 0:
                        record_issue(results, name, "7S", "BIRTHDATE_Future", "Dzimšanas datums ir nākotnē", future_birthdate_count, total_rows)
                        print(f"  Tabulā '{name}': {future_birthdate_count} pacienti ar dzimšanas datumu nākotnē.")
                    else:
                        record_issue(results, name, "7S", "BIRTHDATE_Future", "Dzimšanas datums ir nākotnē - OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "BIRTHDATE_Future", "Dzimšanas datuma nākotnes pārbaude netika veikta (nav DateType vai trūkst kolonnas)", 0, total_rows)

                # JAUNS: Pārbaude, vai nāves datums nav nākotnē
                if "DEATHDATE" in df.columns and df.schema["DEATHDATE"].dataType == DateType():
                    future_deathdate_count = df.filter(
                        (col("DEATHDATE").isNotNull()) & 
                        (col("DEATHDATE") > current_date())
                    ).count()
                    if future_deathdate_count > 0:
                        record_issue(results, name, "7S", "DEATHDATE_Future", "Nāves datums ir nākotnē", future_deathdate_count, total_rows)
                        print(f"  Tabulā '{name}': {future_deathdate_count} pacienti ar nāves datumu nākotnē.")
                    else:
                        record_issue(results, name, "7S", "DEATHDATE_Future", "Nāves datums ir nākotnē - OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "DEATHDATE_Future", "Nāves datuma nākotnes pārbaude netika veikta (nav DateType vai trūkst kolonnas)", 0, total_rows)


                # JAUNS: Pārbaude SSN formātam (tikai cipari)
                if "SSN" in df.columns and df.schema["SSN"].dataType == StringType():
                    non_numeric_ssn_count = df.filter(
                        (col("SSN").isNotNull()) & 
                        (~col("SSN").rlike("^[0-9]+$"))
                    ).count()
                    if non_numeric_ssn_count > 0:
                        record_issue(results, name, "7S", "SSN_Format", "SSN satur ne-ciparu rakstzīmes", non_numeric_ssn_count, total_rows)
                        print(f"  Tabulā '{name}': {non_numeric_ssn_count} SSN vērtības satur ne-ciparu rakstzīmes.")
                    else:
                        record_issue(results, name, "7S", "SSN_Format", "SSN formāts OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "SSN_Format", "SSN formāta pārbaude netika veikta (nav StringType vai trūkst kolonnas)", 0, total_rows)

                # JAUNS: Pārbaude DRIVERS formātam (alfanumeriski ar defisēm)
                if "DRIVERS" in df.columns and df.schema["DRIVERS"].dataType == StringType():
                    invalid_drivers_count = df.filter(
                        (col("DRIVERS").isNotNull()) & 
                        (~col("DRIVERS").rlike("^[A-Za-z0-9-]+$"))
                    ).count()
                    if invalid_drivers_count > 0:
                        record_issue(results, name, "7S", "DRIVERS_Format", "DRIVERS satur nederīgas rakstzīmes", invalid_drivers_count, total_rows)
                        print(f"  Tabulā '{name}': {invalid_drivers_count} DRIVERS vērtības satur nederīgas rakstzīmes.")
                    else:
                        record_issue(results, name, "7S", "DRIVERS_Format", "DRIVERS formāts OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "DRIVERS_Format", "DRIVERS formāta pārbaude netika veikta (nav StringType vai trūkst kolonnas)", 0, total_rows)


            if name == "encounters":
                invalid_start_count = df.filter(
                    (col("START").cast(StringType()) == "INVALID") | 
                    (col("START").isNull())
                ).count()
                if invalid_start_count > 0:
                    record_issue(results, name, "7S", "START", "Nederīga START lauka vērtība (INVALID vai NULL)", invalid_start_count, total_rows)
                    print(f"  Tabulā '{name}': {invalid_start_count} ieraksti ar nederīgu START lauka vērtību.")
                else:
                    record_issue(results, name, "7S", "START", "Nederīga START lauka vērtība - OK", 0, total_rows)
                
                invalid_patient_enc_count = df.filter(
                    (col("PATIENT").cast(StringType()) == "BAD") | 
                    (col("PATIENT").isNull())
                ).count()
                if invalid_patient_enc_count > 0:
                    record_issue(results, name, "7S", "PATIENT", "Nederīga PATIENT lauka vērtība (BAD vai NULL)", invalid_patient_enc_count, total_rows)
                    print(f"  Tabulā '{name}': {invalid_patient_enc_count} ieraksti ar nederīgu PATIENT lauka vērtību.")
                else:
                    record_issue(results, name, "7S", "PATIENT", "Nederīga PATIENT lauka vērtība - OK", 0, total_rows)

                if "START" in df.columns and "STOP" in df.columns and \
                   df.schema["START"].dataType == LongType() and df.schema["STOP"].dataType == LongType():
                    
                    start_stop_order_issue = df.filter(
                        (col("START").isNotNull()) & 
                        (col("STOP").isNotNull()) & 
                        (col("STOP") < col("START"))
                    ).count()
                    if start_stop_order_issue > 0:
                        record_issue(results, name, "7S", "START/STOP", "STOP laiks pirms START laika", start_stop_order_issue, total_rows)
                        print(f"  Tabulā '{name}': {start_stop_order_issue} ieraksti, kur STOP laiks ir pirms START laika.")
                    else:
                        record_issue(results, name, "7S", "START/STOP", "STOP laiks pirms START laika - OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "START/STOP", "START/STOP secības pārbaude netika veikta (nav LongType vai trūkst kolonnas)", 0, total_rows)


            if name == "conditions":
                null_patient_cond_count = df.filter(col("PATIENT").isNull()).count()
                if null_patient_cond_count > 0:
                    record_issue(results, name, "7S", "PATIENT", "PATIENT lauks ir NULL", null_patient_cond_count, total_rows)
                else:
                    record_issue(results, name, "7S", "PATIENT", "PATIENT lauks ir NULL - OK", 0, total_rows)

                invalid_encounter_cond_count = df.filter(col("ENCOUNTER") == "0000").count()
                if invalid_encounter_cond_count > 0:
                    record_issue(results, name, "7S", "ENCOUNTER", "ENCOUNTER lauks ir '0000'", invalid_encounter_cond_count, total_rows)
                    print(f"  Tabulā '{name}': {invalid_encounter_cond_count} ieraksti, kur ENCOUNTER lauks ir '0000'.")
                else:
                    record_issue(results, name, "7S", "ENCOUNTER", "ENCOUNTER lauks ir '0000' - OK", 0, total_rows)

                if "STOP" in df.columns and df.schema["STOP"].dataType == StringType():
                    malformed_stop_date_count = df.filter(
                        (col("STOP").isNotNull()) &
                        (to_date(col("STOP"), "yyyy-MM-dd").isNull()) &
                        (to_date(col("STOP"), "dd/MM/yyyy").isNull()) &
                        (to_date(col("STOP"), "yyyy.MM.dd").isNull()) &
                        (col("STOP").cast(LongType()).isNull())
                    ).count()
                    if malformed_stop_date_count > 0:
                        record_issue(results, name, "7S", "STOP", "Nederīgs datuma formāts (STOP)", malformed_stop_date_count, total_rows)
                        print(f"  Tabulā '{name}': {malformed_stop_date_count} ieraksti ar nederīgu STOP datuma formātu.")
                    else:
                        record_issue(results, name, "7S", "STOP", "Nederīgs datuma formāts (STOP) - OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "STOP", "STOP datuma formāta pārbaude netika veikta (nav StringType vai trūkst kolonnas)", 0, total_rows)

                if "START" in df.columns and "STOP" in df.columns and \
                   df.schema["START"].dataType == LongType() and df.schema["STOP"].dataType == LongType():
                    
                    start_stop_order_issue_cond = df.filter(
                        (col("START").isNotNull()) & 
                        (col("STOP").isNotNull()) & 
                        (col("STOP") < col("START"))
                    ).count()
                    if start_stop_order_issue_cond > 0:
                        record_issue(results, name, "7S", "START/STOP", "STOP laiks pirms START laika", start_stop_order_issue_cond, total_rows)
                        print(f"  Tabulā '{name}': {start_stop_order_issue_cond} ieraksti, kur STOP laiks ir pirms START laika.")
                    else:
                        record_issue(results, name, "7S", "START/STOP", "STOP laiks pirms START laika - OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "START/STOP", "START/STOP secības pārbaude netika veikta (nav LongType vai trūkst kolonnas)", 0, total_rows)


            if name == "medications":
                if "START" in df.columns and df.schema["START"].dataType == StringType():
                    invalid_start_med_count = df.filter(
                        (col("START").isNull()) |
                        (to_date(col("START"), "yyyy-MM-dd").isNull()) |
                        (col("START").cast(LongType()).isNull())
                    ).count()
                    if invalid_start_med_count > 0:
                        record_issue(results, name, "7S", "START", "Nederīga START lauka vērtība (NULL vai String ar nepareizu formātu)", invalid_start_med_count, total_rows)
                        print(f"  Tabulā '{name}': {invalid_start_med_count} ieraksti ar nederīgu START lauka vērtību.")
                    else:
                        record_issue(results, name, "7S", "START", "Nederīga START lauka vērtība - OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "START", "START lauka formāta pārbaude netika veikta (nav StringType vai trūkst kolonnas)", 0, total_rows)

                if "STOP" in df.columns and df.schema["STOP"].dataType == StringType():
                     invalid_stop_med_count = df.filter(
                        (col("STOP").isNull()) |
                        (to_date(col("STOP"), "yyyy-MM-dd").isNull()) |
                        (to_date(col("STOP"), "yyyy.MM.dd").isNull()) |
                        (col("STOP").cast(LongType()).isNull())
                    ).count()
                     if invalid_stop_med_count > 0:
                        record_issue(results, name, "7S", "STOP", "Nederīga STOP lauka vērtība (NULL vai String ar nepareizu formātu)", invalid_stop_med_count, total_rows)
                        print(f"  Tabulā '{name}': {invalid_stop_med_count} ieraksti ar nederīgu STOP lauka vērtību.")
                     else:
                        record_issue(results, name, "7S", "STOP", "Nederīga STOP lauka vērtība - OK", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "STOP", "STOP lauka formāta pārbaude netika veikta (nav StringType vai trūkst kolonnas)", 0, total_rows)

                null_encounter_med_count = df.filter(col("ENCOUNTER").isNull()).count()
                if null_encounter_med_count > 0:
                    record_issue(results, name, "7S", "ENCOUNTER", "ENCOUNTER lauks ir NULL", null_encounter_med_count, total_rows)
                else:
                    record_issue(results, name, "7S", "ENCOUNTER", "ENCOUNTER lauks ir NULL - OK", 0, total_rows)

                null_patient_med_count = df.filter(col("PATIENT").isNull()).count()
                if null_patient_med_count > 0:
                    record_issue(results, name, "7S", "PATIENT", "PATIENT lauks ir NULL", null_patient_med_count, total_rows)
                else:
                    record_issue(results, name, "7S", "PATIENT", "PATIENT lauks ir NULL - OK", 0, total_rows)

                if "START" in df.columns and "STOP" in df.columns:
                    if df.schema["START"].dataType == LongType() and df.schema["STOP"].dataType == LongType():
                        start_stop_order_issue_med = df.filter(
                            (col("START").isNotNull()) & 
                            (col("STOP").isNotNull()) & 
                            (col("STOP") < col("START"))
                        ).count()
                        if start_stop_order_issue_med > 0:
                            record_issue(results, name, "7S", "START/STOP", "STOP laiks pirms START laika", start_stop_order_issue_med, total_rows)
                            print(f"  Tabulā '{name}': {start_stop_order_issue_med} ieraksti, kur STOP laiks ir pirms START laika.")
                        else:
                            record_issue(results, name, "7S", "START/STOP", "STOP laiks pirms START laika - OK", 0, total_rows)
                    else:
                        record_issue(results, name, "7S", "START/STOP", "START/STOP secības pārbaude netika veikta (nav LongType vai trūkst kolonnas)", 0, total_rows)
                else:
                    record_issue(results, name, "7S", "START/STOP", "START/STOP kolonnas trūkst", 0, total_rows)


            if "CODE" in df.columns:
                # Pārbauda, vai CODE atbilst alfanumeriskam formātam ar punktiem un defisēm.
                non_numeric_code_count = df.filter(
                    (col("CODE").isNotNull()) & 
                    (~col("CODE").rlike("^[A-Za-z0-9\.\-]+$")) # Atļauj alfanumeriskus, punktus un defises
                ).count()
                
                if non_numeric_code_count > 0:
                    issue_desc = f"Sintaktiska neatbilstība: Kolonnā 'CODE' atrasti {non_numeric_code_count} kodi, kas neatbilst paredzētajam alfanumeriskajam formātam."
                    record_issue(results, name, "7S", "CODE_AlphanumericFormat", issue_desc, non_numeric_code_count, total_rows)
                    print(f"  {issue_desc}")
                else:
                    print(f"  Sintakse OK: Visi 'CODE' lauki atbilst paredzētajam alfanumeriskajam formātam (ja eksistē).")
                    record_issue(results, name, "7S", "CODE_AlphanumericFormat", "Visi kodi atbilst paredzētajai vārdnīcai", 0, total_rows)
            else:
                record_issue(results, name, "7S", "CODE_AlphanumericFormat", "Kolonna 'CODE' trūkst", 0, total_rows)
        else:
            print(f"  Domēnspecifiskās sintaktiskās validācijas pārbaudes netika veiktas, jo domēns nav 'Veselības aprūpe'.")
            record_issue(results, name, "7S", "Domain_Specific_Syntactic_Checks", "Domēnspecifiskās sintaktiskās validācijas pārbaudes izlaistas (nav 'Veselības aprūpe' domēns)", 0, total_rows)


        # --- 9. posms: Noteikumu balstīta validācija (biznesa loģika, integritāte) ---
        print(f"\n--- 9. posms: Noteikumu balstīta validācija tabulai: '{name}' ---")

        # Atsauces integritātes pārbaudes
        # Šīs pārbaudes veic tiešās atsauces integritātes pārbaudes.
        # Sarežģītākas, tranzītas atkarības (piemēram, "trīs tabulu variants")
        # parasti prasa specifiskas biznesa loģikas implementāciju, kas nav iekļauta automatizētā veidā šajā posmā.
        if determined_domain == "Veselības aprūpe": # Izpilda tikai veselības aprūpes domēnam
            if name in reference_rules:
                for fk_col, ref_table_name, pk_col in reference_rules[name]:
                    if ref_table_name in df_map:
                        ref_df = df_map[ref_table_name]
                        
                        if fk_col in df.columns and pk_col in ref_df.columns:
                            invalid_fk_count = df.filter(col(fk_col).isNotNull()) \
                                               .join(ref_df, 
                                                     lower(col(fk_col)) == lower(ref_df[pk_col]), 
                                                     "left_anti") \
                                               .count()
                            
                            if invalid_fk_count > 0:
                                issue_desc = f"Atsauces integritātes pārkāpums: Kolonna '{fk_col}' satur {invalid_fk_count} vērtības, kas neeksistē '{ref_table_name}.{pk_col}'."
                                record_issue(results, name, "8RI", fk_col, issue_desc, invalid_fk_count, total_rows)
                                print(f"  {issue_desc}")
                            else:
                                print(f"  Atsauces integritāte OK: Kolonna '{fk_col}' uz '{ref_table_name}.{pk_col}'.")
                                record_issue(results, name, "8RI", fk_col, f"Atsauces integritāte OK uz '{ref_table_name}.{pk_col}'", 0, total_rows)

                        else:
                            print(f"  Brīdinājums: Nevar pārbaudīt atsauces integritāti '{name}.{fk_col}' uz '{ref_table_name}.{pk_col}', jo trūkst vienas vai abas kolonnas.")
                            record_issue(results, name, "8RI", fk_col, f"Atsauces integritātes pārbaude netika veikta (trūkst kolonnas)", 1, 1)
                    else:
                        print(f"  Brīdinājums: Atsauces tabula '{ref_table_name}' nav ielādēta. Nevar pārbaudīt atsauces integritāti no '{name}.{fk_col}'.")
                        record_issue(results, name, "8RI", fk_col, f"Atsauces tabula '{ref_table_name}' nav ielādēta", 1, 1)
            else:
                record_issue(results, name, "8RI", "Referential_Integrity_Checks", "Atsauces integritātes noteikumi nav definēti šai tabulai", 0, total_rows)


            # Biznesa loģikas pārbaudes
            if name == "allergies":
                if "patients" in df_map and "PATIENT" in df.columns and "START" in df.columns:
                    patients_df = df_map["patients"].select(col("ID").alias("patient_id"), col("DEATHDATE").alias("patient_deathdate"))
                    
                    joined_df = df.alias("a").join(patients_df.alias("p"), 
                                                  lower(col("a.PATIENT")) == lower(col("p.patient_id")), 
                                                  "inner") \
                                  .filter(col("a.START").isNotNull() & col("p.patient_deathdate").isNotNull()) \
                                  .withColumn("start_date", to_date(col("a.START").cast(StringType()), "yyyy-MM-dd")) \
                                  .withColumn("death_date", to_date(col("p.patient_deathdate"), "yyyy-MM-dd"))
                    
                    allergies_after_death_count = joined_df.filter(
                        col("start_date").isNotNull() & col("death_date").isNotNull() &
                        (col("start_date") > col("death_date"))
                    ).count()

                    if allergies_after_death_count > 0:
                        issue_desc = f"Biznesa loģikas pārkāpums: {allergies_after_death_count} alerģijas ieraksti reģistrēti pēc pacienta nāves datuma."
                        record_issue(results, name, "8BL", "START/DEATHDATE", issue_desc, allergies_after_death_count, total_rows)
                        print(f"  {issue_desc}")
                    else:
                        print(f"  Biznesa loģika OK: Nav alerģijas ierakstu pēc pacienta nāves datuma.")
                        record_issue(results, name, "8BL", "START/DEATHDATE", "Nav alerģijas ierakstu pēc nāves datuma", 0, total_rows)
                else:
                    print(f"  Brīdinājums: Nevar veikt 'alerģijas pēc nāves' pārbaudi tabulai '{name}', jo trūkst 'patients' tabula vai nepieciešamās kolonnas.")
                    record_issue(results, name, "8BL", "START/DEATHDATE", "Pārbaude netika veikta (trūkst atkarību)", 1, 1)

            if name == "medications":
                if "patients" in df_map and "PATIENT" in df.columns and "START" in df.columns:
                    patients_df = df_map["patients"].select(col("ID").alias("patient_id"), col("DEATHDATE").alias("patient_deathdate"))
                    
                    joined_df = df.alias("m").join(patients_df.alias("p"), 
                                                  lower(col("m.PATIENT")) == lower(col("p.patient_id")), 
                                                  "inner") \
                                  .filter(col("m.START").isNotNull() & col("p.patient_deathdate").isNotNull()) \
                                  .withColumn("start_date", to_date(col("m.START").cast(StringType()), "yyyy-MM-dd")) \
                                  .withColumn("death_date", to_date(col("p.patient_deathdate"), "yyyy-MM-dd"))
                    
                    medications_after_death_count = joined_df.filter(
                        col("start_date").isNotNull() & col("death_date").isNotNull() &
                        (col("start_date") > col("death_date"))
                    ).count()

                    if medications_after_death_count > 0:
                        issue_desc = f"Biznesa loģikas pārkāpums: {medications_after_death_count} medikamentu ieraksti reģistrēti pēc pacienta nāves datuma."
                        record_issue(results, name, "8BL", "START/DEATHDATE", issue_desc, medications_after_death_count, total_rows)
                        print(f"  {issue_desc}")
                    else:
                        print(f"  Biznesa loģika OK: Nav medikamentu ierakstu pēc pacienta nāmes datura.")
                        record_issue(results, name, "8BL", "START/DEATHDATE", "Nav medikamentu ierakstu pēc nāves datuma", 0, total_rows)
                else:
                    print(f"  Brīdinājums: Nevar veikt 'medikamenti pēc nāves' pārbaudi tabulai '{name}', jo trūkst 'patients' tabula vai nepieciešamās kolonnas.")
                    record_issue(results, name, "8BL", "START/DEATHDATE", "Pārbaude netika veikta (trūkst atkarību)", 1, 1)
            else: # Šis 'else' bija nepareizi novietots, attiecinot uz 'medications', nevis vispārīgi
                # Lai saglabātu oriģinālo loģiku, pievienojam vispārīgu ziņojumu, ja nav specifisku biznesa loģikas pārbaužu
                if name not in ["allergies", "medications"]: # Ja tabulai nav specifisku BL pārbaužu
                     record_issue(results, name, "8BL", "Business_Logic_Checks", "Biznesa loģikas pārbaudes nav definētas šai tabulai", 0, total_rows)
        else:
            print(f"  Biznesa loģikas un atsauces integritātes pārbaudes netika veiktas, jo domēns nav 'Veselības aprūpe'.")
            record_issue(results, name, "8BL", "Domain_Specific_Business_Logic_Checks", "Domēnspecifiskās biznesa loģikas pārbaudes izlaistas (nav 'Veselības aprūpe' domēns)", 0, total_rows)
            record_issue(results, name, "8RI", "Domain_Specific_Referential_Integrity_Checks", "Domēnspecifiskās atsauces integritātes pārbaudes izlaistas (nav 'Veselības aprūpe' domēns)", 0, total_rows)


        # --- 10. posms: Semantiskā/Ontoloģiskā validācija (terminoloģija, kodu vārdnīcas) ---
        print(f"\n--- 10. posms: Semantiskā/Ontoloģiskā validācija tabulai: '{name}' ---")

        if determined_domain == "Veselības aprūpe": # Izpilda tikai veselības aprūpes domēnam
            if name == "conditions":
                # Definējam SNOMED CT kodus un aprakstus no jūsu attēla
                # Mēs izmantojam aprakstu kā atslēgu un kodu kā vērtību, lai varētu meklēt kodu pēc apraksta.
                snomed_ct_ontology = {
                    "Received certificate of high school equivalency (finding)": "105480006",
                    "Chronic neck pain (finding)": "279039007",
                    "Acute deep venous thrombosis (disorder)": "128053003",
                    "Nonproliferative diabetic retinopathy due to type 2 diabetes mellitus (disorder)": "399270008",
                    "Neuropathy due to type 2 diabetes mellitus (disorder)": "230572002",
                    "Chronic intractable migraine without aura (disorder)": "719080004", # Pievienots (disorder) beigās, ja tas ir bieži sastopams sufikss
                    "Microalbuminuria due to type 2 diabetes mellitus (disorder)": "408540003",
                    "Macular edema and retinopathy due to type 2 diabetes mellitus (disorder)": "422034002",
                    "Proliferative diabetic retinopathy due to type II diabetes mellitus (disorder)": "399304008",
                }
                
                snomed_broadcast = spark.sparkContext.broadcast(snomed_ct_ontology)

                # 10.1 Kodu derīguma pārbaude (pret SNOMED CT vārdnīcu)
                # Pārbaudām, vai CODE vērtība (ja tā ir ciparu virkne) atbilst kādam no zināmajiem SNOMED CT kodiem
                
                # Šeit joprojām pārbaudām SNOMED CT kodu atbilstību, bet balstoties uz to, vai kods ir atrodams ontoloģijas vērtībās.
                # Ja CODE ir ciparu formātā, bet nav zināmajā SNOMED CT kodu sarakstā, tad tā ir semantiska neatbilstība.
                invalid_snomed_code_count = df.filter(
                    (col("CODE").isNotNull()) & 
                    (col("CODE").rlike("^[0-9]+$")) & # Pārliecināmies, ka kods ir ciparu virkne
                    (~col("CODE").isin(list(snomed_ct_ontology.values()))) # Pārbauda, vai CODE nav zināmo kodu sarakstā
                ).count()

                if invalid_snomed_code_count > 0:
                    issue_desc = f"Semantiska neatbilstība: Kolonnā 'CODE' atrasti {invalid_snomed_code_count} kodi (ciparu formātā), kas neatbilst definētajiem SNOMED CT kodiem."
                    record_issue(results, name, "9S", "CODE_SNOMED_Validity", issue_desc, invalid_snomed_code_count, total_rows)
                    print(f"  {issue_desc}")
                else:
                    print(f"  Semantika OK: Visi 'CODE' lauki (ja ciparu formātā) atbilst definētajiem SNOMED CT kodiem (pēc pieejamās vārdnīcas).")
                    record_issue(results, name, "9S", "CODE_SNOMED_Validity", "Visi kodi atbilst definētajai vārdnīcai", 0, total_rows)

                # 10.2 Semantiskā saskaņošana (kodu "labošana")
                from pyspark.sql.types import StringType
                from pyspark.sql.functions import udf

                def get_snomed_code_by_description(description, snomed_map):
                    if description:
                        # Normalizējam aprakstu, lai atbilstu vārdnīcas atslēgām
                        # Noņemam bieži sastopamus SNOMED CT sufiksus un normalizējam uz mazajiem burtiem.
                        clean_description = description.strip().replace(" (finding)", "").replace(" (disorder)", "").lower()
                        
                        # Izveidojam apgriezto vārdnīcu, lai meklētu pēc apraksta.
                        # SNOMED_CT_ONTOLOGY ir (apraksts -> kods). Tā ir pareizā forma šim meklēšanai.
                        # Pārliecināmies, ka vārdnīcas atslēgas ir mazajiem burtiem salīdzināšanai.
                        normalized_snomed_dict_keys = {
                            k.strip().replace(" (finding)", "").replace(" (disorder)", "").lower(): v 
                            for k, v in snomed_map.value.items()
                        }

                        if clean_description in normalized_snomed_dict_keys:
                            return normalized_snomed_dict_keys[clean_description]
                    return None # Atgriežam None, ja nav atbilstības vai apraksts ir tukšs

                # Reģistrējam UDF
                get_snomed_code_udf = udf(lambda desc: get_snomed_code_by_description(desc, snomed_broadcast), StringType())

                # Filtrējam rindas, kur CODE ir nederīgs (NULL vai nav ciparu formātā) UN ir DESCRIPTION.
                df_to_correct_semantic = df.filter(
                    (col("DESCRIPTION").isNotNull()) & 
                    (
                        (col("CODE").isNull()) | 
                        (~col("CODE").rlike("^[0-9]+$"))
                    )
                )
                
                if "DESCRIPTION" in df.columns:
                    corrected_df_semantic = df_to_correct_semantic.withColumn("SUGGESTED_CODE", get_snomed_code_udf(col("DESCRIPTION")))
                    
                    # Saskaitām rindas, kurām varēja ieteikt kodu
                    suggested_code_count = corrected_df_semantic.filter(col("SUGGESTED_CODE").isNotNull()).count()

                    if suggested_code_count > 0:
                        issue_desc = f"Semantiskā labošana: {suggested_code_count} 'CODE' vērtības varētu labot, izmantojot 'DESCRIPTION' kolonnu un SNOMED CT ontoloģiju."
                        record_issue(results, name, "9S", "CODE_Correction_by_Description", issue_desc, suggested_code_count, total_rows)
                        print(f"  {issue_desc}")
                    else:
                        print(f"  Semantiskā labošana: Netika atrasti labojumi 'CODE' vērtībām, izmantojot 'DESCRIPTION' un SNOMED CT ontoloģiju.")
                        record_issue(results, name, "9S", "CODE_Correction_by_Description", "Netika atrasti labojumi", 0, total_rows)
                else:
                    print(f"  Brīdinājums: Kolonna 'DESCRIPTION' nav pieejama tabulā '{name}'. Nevar veikt semantiskās labošanas mēģinājumu, izmantojot aprakstu.")
                    record_issue(results, name, "9S", "CODE_Correction_by_Description", "Kolonna 'DESCRIPTION' nav pieejama", 1, 1)
            else:
                record_issue(results, name, "9S", "Semantic_Ontological_Checks", "Semantiskās/Ontoloģiskās validācijas pārbaudes nav definētas šai tabulai", 0, total_rows)
        else:
            print(f"  Semantiskās/Ontoloģiskās validācijas pārbaudes netika veiktas, jo domēns nav 'Veselības aprūpe'.")
            record_issue(results, name, "9S", "Domain_Specific_Semantic_Checks", "Domēnspecifiskās semantiskās/ontoloģiskās validācijas pārbaudes izlaistas (nav 'Veselības aprūpe' domēns)", 0, total_rows)


        # --- 11. posms: Klīniskās loģikas pārbaudes (saderība, secība - padziļināti) ---
        print(f"\n--- 11. posms: Klīniskās loģikas pārbaudes tabulai: '{name}' ---")

        if determined_domain == "Veselības aprūpe": # Izpilda tikai veselības aprūpes domēnam
            if name == "conditions" and "patients" in df_map:
                conditions_df = df.alias("c")
                patients_df = df_map["patients"].alias("p").select(col("ID").alias("patient_id"), col("GENDER").alias("patient_gender"), col("BIRTHDATE").alias("patient_birthdate"))

                # Apvienojam conditions ar patients, lai piekļūtu dzimuma un dzimšanas datuma informācijai
                joined_clinical_df = conditions_df.join(patients_df, lower(conditions_df.PATIENT) == lower(patients_df.patient_id), "inner")

                # 11.1 Pārbaude: Grūtniecības stāvoklis vīriešiem
                # Meklējam aprakstā vārdu "pregnancy" (neatkarīgi no reģistra) un pārbaudām, vai dzimums ir "M"
                male_pregnancy_count = joined_clinical_df.filter(
                    (lower(col("DESCRIPTION")).contains("pregnancy")) & 
                    (lower(col("patient_gender")) == "m")
                ).count()

                if male_pregnancy_count > 0:
                    issue_desc = f"Klīniskās loģikas pārkāpums: {male_pregnancy_count} 'conditions' ieraksti norāda uz grūtniecību vīriešu dzimuma pacientiem."
                    record_issue(results, name, "10CL", "Male_Pregnancy", issue_desc, male_pregnancy_count, total_rows)
                    print(f"  {issue_desc}")
                else:
                    print(f"  Klīniskā loģika OK: Nav grūtniecības ierakstu vīriešu dzimuma pacientiem.")
                    record_issue(results, name, "10CL", "Male_Pregnancy", "Nav grūtniecības ierakstu vīriešu dzimuma pacientiem", 0, total_rows)

                # 11.2 Pārbaude: 2. tipa cukura diabēts ļoti jauniem pacientiem
                # Pieņemam, ka ļoti jauns ir dzimis pēc 2015. gada (piemērs, lai demonstrētu loģiku)
                # Reālā situācijā būtu nepieciešams precīzāks vecuma aprēķins un klīniskās vadlīnijas.
                young_diabetes_count = joined_clinical_df.filter(
                    (lower(col("DESCRIPTION")).contains("type 2 diabetes mellitus")) &
                    (col("patient_birthdate").isNotNull()) &
                    (col("patient_birthdate") > to_date(lit("2015-01-01"), "yyyy-MM-dd")) # Pārbauda, vai dzimis pēc 2015. gada
                ).count()

                if young_diabetes_count > 0:
                    issue_desc = f"Klīniskās loģikas pārkāpums: {young_diabetes_count} 'conditions' ieraksti norāda uz 2. tipa cukura diabētu pacientiem, kas dzimuši pēc 2015. gada."
                    record_issue(results, name, "10CL", "Young_Type2_Diabetes", issue_desc, young_diabetes_count, total_rows)
                    print(f"  {issue_desc}")
                else:
                    print(f"  Klīniskā loģika OK: Nav 2. tipa cukura diabēta ierakstu ļoti jauniem pacientiem.")
                    record_issue(results, name, "10CL", "Young_Type2_Diabetes", "Nav 2. tipa cukura diabēta ierakstu ļoti jauniem pacientiem", 0, total_rows)

            else:
                print(f"  Brīdinājums: Nevar veikt klīniskās loģikas pārbaudes tabulai '{name}', jo trūkst 'conditions' vai 'patients' tabula, vai nepieciešamās kolonnas.")
                record_issue(results, name, "10CL", "Clinical_Logic_Checks", "Klīniskās loģikas pārbaudes netika veiktas (trūkst atkarību)", 1, 1)
        else:
            print(f"  Klīniskās loģikas pārbaudes netika veiktas, jo domēns nav 'Veselības aprūpe'.")
            record_issue(results, name, "10CL", "Domain_Specific_Clinical_Logic_Checks", "Domēnspecifiskās klīniskās loģikas pārbaudes izlaistas (nav 'Veselības aprūpe' domēns)", 0, total_rows)


        # --- 12. posms: Starptabulu saistību un trūkstošo lauku aizpildīšana (Referential Integrity and Imputation) ---
        # ŠIS POSMS IR NOŅEMTS, KĀ PRASĪTS
        print(f"\n--- 12. posms: Starptabulu saistību un trūkstošo lauku aizpildīšanas risinājums ir noņemts. Šis posms tiek izlaists tabulai '{name}' ---")
        record_issue(results, name, "11FI", "Cross_Table_Imputation_Removed", "Starptabulu trūkstošo lauku aizpildīšanas risinājums noņemts, solis izlaists.", 0, total_rows if total_rows else 1)


        # --- 13. posms: Datu pilnīguma un savlaicīguma pārbaudes (trūkstošie dati, datu vecums) ---
        print(f"\n--- 13. posms: Datu pilnīguma un savlaicīguma pārbaudes tabulai: '{name}' ---")

        # 13.1 Vispārēja trūkstošo vērtību procentuālā pārbaude
        for col_name in df.columns:
            # Jauns: Pārbaudām, vai kolonna ir definēta kā "nullable" pašreizējā domēnā un tabulā.
            is_nullable_by_domain = False
            if determined_domain != "Nenoteikts" and \
               name in domain_definitions[determined_domain]["nullable_columns"] and \
               col_name in domain_definitions[determined_domain]["nullable_columns"][name]:
                is_nullable_by_domain = True

            if is_nullable_by_domain:
                print(f"  Pilnīguma pārbaude izlaista kolonnai '{col_name}' tabulā '{name}', jo saskaņā ar '{determined_domain}' domēna definīciju NULL vērtības ir sagaidāmas.")
                record_issue(results, name, "12C", col_name, f"Pilnīgums OK: NULL vērtības ir sagaidāmas (domēnspecifiski).", 0, total_rows)
                continue # Pārejam pie nākamās kolonnas

            null_count = df.filter(col(col_name).isNull()).count()
            if total_rows > 0:
                null_percentage = (null_count / total_rows) * 100
                if null_percentage > 0:
                    issue_desc = f"Pilnīguma pārkāpums: Kolonnai '{col_name}' ir {null_count} trūkstošās vērtības ({null_percentage:.2f}%)."
                    record_issue(results, name, "12C", col_name, issue_desc, null_count, total_rows)
                    print(f"  {issue_desc}")
                else:
                    record_issue(results, name, "12C", col_name, f"Pilnīgums OK: Kolonnai '{col_name}' nav trūkstošo vērtību.", 0, total_rows)
                    print(f"  Pilnīgums OK: Kolonnai '{col_name}' nav trūkstošo vērtību.")
            else:
                record_issue(results, name, "12C", col_name, f"Pilnīguma pārbaude netika veikta (tukša tabula).", 0, 0)
                print(f"  Pilnīguma pārbaude netika veikta kolonnai '{col_name}' (tukša tabula).")

        # 13.2 Datu savlaicīguma pārbaude (ieraksti vecāki par 5 gadiem)
        # Pārbaudām START vai STOP kolonnas, ja tās ir pieejamas un ir datuma tips (vai var tikt pārveidotas)
        date_column_to_check = None
        # Pārbaudām, vai START vai STOP ir StringType, lai varētu mēģināt pārveidot
        if "STOP" in df.columns and df.schema["STOP"].dataType == StringType():
            date_column_to_check = "STOP"
        elif "START" in df.columns and df.schema["START"].dataType == StringType():
            date_column_to_check = "START"
        
        if date_column_to_check:
            # Mēģinām pārveidot datumu, atbalstot vairākus formātus
            df_with_date = df.withColumn(
                "event_date", 
                coalesce(
                    to_date(col(date_column_to_check), "yyyy-MM-dd"),
                    to_date(col(date_column_to_check), "dd/MM/yyyy"), # Pievienots dd/MM/yyyy formāts
                    to_date(col(date_column_to_check), "yyyy.MM.dd")
                )
            )
            
            # Pārbaudām, cik ierakstu ir vecāki par 5 gadiem
            five_years_ago = date_sub(current_date(), 365 * 5) # Aptuveni 5 gadi atpakaļ

            outdated_records_count = df_with_date.filter(
                (col("event_date").isNotNull()) & 
                (col("event_date") < five_years_ago)
            ).count()

            if outdated_records_count > 0:
                issue_desc = f"Savlaicīguma pārkāpums: {outdated_records_count} ieraksti ir vecāki par 5 gadiem (balstoties uz '{date_column_to_check}' kolonnu)."
                record_issue(results, name, "12T", "Timeliness", issue_desc, outdated_records_count, total_rows)
                print(f"  {issue_desc}")
            else:
                print(f"  Savlaicīgums OK: Nav ierakstu, kas būtu vecāki par 5 gadiem (balstoties uz '{date_column_to_check}' kolonnu).")
                record_issue(results, name, "12T", "Timeliness", "Nav ierakstu, kas būtu vecāki par 5 gadiem", 0, total_rows)
        else:
            print(f"  Brīdinājums: Nevar veikt savlaicīguma pārbaudi tabulai '{name}', jo trūkst derīgas datuma kolonnas (START vai STOP) vai tā nav StringType.")
            record_issue(results, name, "12T", "Timeliness", "Savlaicīguma pārbaude netika veikta (trūkst datuma kolonnas vai nav StringType)", 1, 1)


        df.unpersist() 
        print(f"  Tabulas '{name}' kešatmiņa atbrīvota.")
    
    # --- 15. posms: Datu labošana ---
    print("\n--- 15. posms: Datu labošana ---")
    corrected_df_map = {}

    for name, df in df_map.items():
        print(f"\nVeicu datu labošanu tabulai: '{name}'")
        corrected_df = df

        # 15.1. Pilnīgums: Trūkstošo vērtību apstrāde (Aizpilda ar "UNKNOWN" vai 0)
        # Šis solis aizpilda NULL vērtības, lai uzlabotu datu pilnīgumu.
        # Tomēr, ja šīs vērtības ir svešās atslēgas (FK), to aizpildīšana ar "UNKNOWN" nenodrošina atsauces integritāti.
        # Atsauces integritātes labošana tiek veikta 15.5. posmā.
        for col_name, dtype in corrected_df.dtypes:
            if dtype == "string":
                corrected_df = corrected_df.withColumn(col_name, when(col(col_name).isNull(), lit("UNKNOWN")).otherwise(col(col_name)))
            elif dtype in ["int", "long", "double", "float"]:
                corrected_df = corrected_df.withColumn(col_name, when(col(col_name).isNull(), lit(0)).otherwise(col(col_name)))
        print(f"  Kolonnās: Trūkstošās vērtības automātiski aizpildītas ar 'UNKNOWN' (virknes) vai '0' (skaitļi).")
        record_issue(results, name, "15C", "Completeness_Auto_Fill", "Trūkstošās vērtības automātiski aizpildītas.", 0, corrected_df.count())


        # 15.2. Unikalitāte: Noņem precīzus rindu dublikātus
        # Saskaņā ar analīzi, dublikāti tiek uzskatīti tikai rindas līmenī (ja visa rinda ir identiska).
        # Šis solis efektīvi noņem šādus precīzus rindu dublikātus.
        initial_count_exact_duplicates = corrected_df.count()
        corrected_df = corrected_df.dropDuplicates() # Noņem precīzus rindu dublikātus (identiskas rindas)
        exact_duplicates_removed = initial_count_exact_duplicates - corrected_df.count()
        if exact_duplicates_removed > 0:
            print(f"  Noņemti {exact_duplicates_removed} precīzi rindu dublikāti (identiskas rindas).")
            record_issue(results, name, "15C", "Exact_Duplicates_Removed", f"Noņemti {exact_duplicates_removed} precīzi rindu dublikāti.", exact_duplicates_removed, initial_count_exact_duplicates)
        else:
            print(f"  Precīzi rindu dublikāti nav atrasti.")
            record_issue(results, name, "15C", "Exact_Duplicates_Removed", "Precīzi rindu dublikāti nav atrasti.", 0, initial_count_exact_duplicates)


        # 15.3. Precizitāte/Savlaicīgums: Datumu un dzimuma labošana
        if name == "patients":
            if "GENDER" in corrected_df.columns:
                corrected_df = corrected_df.withColumn(
                    "GENDER",
                    when(lower(col("GENDER")).isin("m", "f"), upper(col("GENDER"))).otherwise("U")
                )
                print(f"  Kolonnā 'GENDER': Nederīgās vērtības labotas uz 'U'.")
                record_issue(results, name, "15C", "GENDER_Corrected", "Nederīgās dzimuma vērtības labotas.", 0, corrected_df.count())

            if "BIRTHDATE" in corrected_df.columns and "DEATHDATE" in corrected_df.columns:
                # Pārveido datumu virknes uz datuma tipu, ja nepieciešams, izmantojot vairākus formātus
                corrected_df = corrected_df.withColumn(
                    "BIRTHDATE_DT",
                    coalesce(
                        to_date(col("BIRTHDATE"), "yyyy-MM-dd"),
                        to_date(col("BIRTHDATE"), "MM/dd/yyyy"),
                        to_date(col("BIRTHDATE"), "yyyy.MM.dd")
                    )
                ).withColumn(
                    "DEATHDATE_DT",
                    coalesce(
                        to_date(col("DEATHDATE"), "yyyy-MM-dd"),
                        to_date(col("DEATHDATE"), "MM/dd/yyyy"),
                        to_date(col("DEATHDATE"), "yyyy.MM.dd")
                    )
                )
                
                # Labo gadījumus, kad DEATHDATE ir pirms BIRTHDATE
                corrected_df = corrected_df.withColumn(
                    "DEATHDATE_DT",
                    when(
                        (col("BIRTHDATE_DT").isNotNull()) & 
                        (col("DEATHDATE_DT").isNotNull()) & 
                        (col("DEATHDATE_DT") < col("BIRTHDATE_DT")),
                        col("BIRTHDATE_DT") # Ja nāves datums ir pirms dzimšanas datuma, iestata to uz dzimšanas datumu
                    ).otherwise(col("DEATHDATE_DT"))
                )

                # JAUNS: Labo gadījumus, kad BIRTHDATE vai DEATHDATE ir nākotnē
                corrected_df = corrected_df.withColumn(
                    "BIRTHDATE_DT",
                    when(
                        (col("BIRTHDATE_DT").isNotNull()) & 
                        (col("BIRTHDATE_DT") > current_date()),
                        current_date() # Ja dzimšanas datums ir nākotnē, iestata to uz šodienas datumu
                    ).otherwise(col("BIRTHDATE_DT"))
                ).withColumn(
                    "DEATHDATE_DT",
                    when(
                        (col("DEATHDATE_DT").isNotNull()) & 
                        (col("DEATHDATE_DT") > current_date()),
                        current_date() # Ja nāves datums ir nākotnē, iestata to uz šodienas datumu
                    ).otherwise(col("DEATHDATE_DT"))
                )

                # Atjaunina oriģinālās kolonnas
                corrected_df = corrected_df.withColumn("BIRTHDATE", date_format(col("BIRTHDATE_DT"), "yyyy-MM-dd")) \
                                           .withColumn("DEATHDATE", date_format(col("DEATHDATE_DT"), "yyyy-MM-dd")) \
                                           .drop("BIRTHDATE_DT", "DEATHDATE_DT")
                print(f"  Kolonnās 'BIRTHDATE'/'DEATHDATE': Laboti nederīgi datumi, secības problēmas un datumi nākotnē.")
                record_issue(results, name, "15C", "Dates_Corrected", "Datumu secības, formāta un nākotnes datumu problēmas labotas.", 0, corrected_df.count())
            else:
                print(f"  Brīdinājums: 'BIRTHDATE' vai 'DEATHDATE' kolonnas nav pieejamas tabulā '{name}'. Datumu labošana izlaista.")
                record_issue(results, name, "15C", "Dates_Corrected", "Datumu labošana izlaista (trūkst kolonnas).", 0, corrected_df.count())

            # JAUNS: SSN un DRIVERS formāta labošana
            if "SSN" in corrected_df.columns and corrected_df.schema["SSN"].dataType == StringType():
                # Noņem visus ne-ciparu rakstzīmes no SSN
                corrected_df = corrected_df.withColumn(
                    "SSN",
                    when(col("SSN").isNotNull(), regexp_replace(col("SSN"), "[^0-9]", "")).otherwise(None)
                )
                # Ja pēc tīrīšanas SSN ir tukšs, iestata to uz "INVALID_FORMAT"
                corrected_df = corrected_df.withColumn(
                    "SSN",
                    when(col("SSN") == "", lit("INVALID_FORMAT")).otherwise(col("SSN"))
                )
                print(f"  Kolonnā 'SSN': Noņemti ne-ciparu rakstzīmes un tukšās vērtības labotas uz 'INVALID_FORMAT'.")
                record_issue(results, name, "15C", "SSN_Format_Corrected", "SSN formāts labots (tikai cipari).", 0, corrected_df.count())
            else:
                print(f"  Brīdinājums: Kolonna 'SSN' nav pieejama vai nav StringType tabulā '{name}'. SSN formāta labošana izlaista.")
                record_issue(results, name, "15C", "SSN_Format_Corrected", "SSN formāta labošana izlaista (trūkst kolonnas vai nav StringType).", 0, corrected_df.count())

            if "DRIVERS" in corrected_df.columns and corrected_df.schema["DRIVERS"].dataType == StringType():
                # Noņem visus rakstzīmes, kas nav alfanumeriskas vai defises
                corrected_df = corrected_df.withColumn(
                    "DRIVERS",
                    when(col("DRIVERS").isNotNull(), regexp_replace(col("DRIVERS"), "[^A-Za-z0-9-]", "")).otherwise(None)
                )
                # Ja pēc tīrīšanas DRIVERS ir tukšs, iestata to uz "INVALID_FORMAT"
                corrected_df = corrected_df.withColumn(
                    "DRIVERS",
                    when(col("DRIVERS") == "", lit("INVALID_FORMAT")).otherwise(col("DRIVERS"))
                )
                print(f"  Kolonnā 'DRIVERS': Noņemti nederīgas rakstzīmes un tukšās vērtības labotas uz 'INVALID_FORMAT'.")
                record_issue(results, name, "15C", "DRIVERS_Format_Corrected", "DRIVERS formāts labots (alfanumeriski ar defisēm).", 0, corrected_df.count())
            else:
                print(f"  Brīdinājums: Kolonna 'DRIVERS' nav pieejama vai nav StringType tabulā '{name}'. DRIVERS formāta labošana izlaista.")
                record_issue(results, name, "15C", "DRIVERS_Format_Corrected", "DRIVERS formāta labošana izlaista (trūkst kolonnas vai nav StringType).", 0, corrected_df.count())


        if name in ["encounters", "conditions", "medications", "allergies"]:
            if "START" in corrected_df.columns and "STOP" in corrected_df.columns:
                # Pārveido LongType uz datuma tipu, pieņemot, ka tas irYYYYMMDD formāts
                # Pievienots atbalsts dd/MM/yyyy formātam, kas ir norādīts jautājumā
                corrected_df = corrected_df.withColumn(
                    "START_DT",
                    coalesce(
                        to_date(col("START").cast(StringType()), "yyyyMMdd"),
                        to_date(col("START").cast(StringType()), "yyyy-MM-dd"), # Atbalsta arī string formātu
                        to_date(col("START").cast(StringType()), "dd/MM/yyyy")  # JAUNS: dd/MM/yyyy formāts
                    )
                ).withColumn(
                    "STOP_DT",
                    coalesce(
                        to_date(col("STOP").cast(StringType()), "yyyyMMdd"),
                        to_date(col("STOP").cast(StringType()), "yyyy-MM-dd"),
                        to_date(col("STOP").cast(StringType()), "dd/MM/yyyy")  # JAUNS: dd/MM/yyyy formāts
                    )
                )
                
                # Labo gadījumus, kad STOP ir pirms START
                corrected_df = corrected_df.withColumn(
                    "STOP_DT",
                    when(
                        (col("START_DT").isNotNull()) & 
                        (col("STOP_DT").isNotNull()) & 
                        (col("STOP_DT") < col("START_DT")),
                        col("START_DT") # Ja STOP datums ir pirms START datuma, iestata to uz START datumu
                    ).otherwise(col("STOP_DT"))
                )
                # Atjaunina oriģinālās kolonnas
                corrected_df = corrected_df.withColumn("START", date_format(col("START_DT"), "yyyy-MM-dd")) \
                                           .withColumn("STOP", date_format(col("STOP_DT"), "yyyy-MM-dd")) \
                                           .drop("START_DT", "STOP_DT")
                print(f"  Kolonnās 'START'/'STOP': Laboti nederīgi datumi un secības problēmas.")
                record_issue(results, name, "15C", "StartStop_Dates_Corrected", "Datumu secības un formāta problēmas labotas.", 0, corrected_df.count())
            else:
                print(f"  Brīdinājums: 'START' vai 'STOP' kolonnas nav pieejamas tabulā '{name}'. Datumu labošana izlaista.")
                record_issue(results, name, "15C", "StartStop_Dates_Corrected", "Datumu labošana izlaista (trūkst kolonnas).", 0, corrected_df.count())

        # 15.4. Derīgums: CODE kolonnas labošana (tikai cipari)
        if "CODE" in corrected_df.columns:
            # Pārbauda, vai CODE satur tikai ciparus. Ja satur burtus, to nomaina uz "UNKNOWN".
            corrected_df = corrected_df.withColumn(
                "CODE",
                when(col("CODE").rlike("^[0-9]+$"), col("CODE")).otherwise("UNKNOWN")
            )
            print(f"  Kolonnā 'CODE': Vērtības, kas nav tikai cipari, labotas uz 'UNKNOWN'.")
            record_issue(results, name, "15C", "CODE_Format_Corrected", "Vērtības, kas nav tikai cipari, labotas uz 'UNKNOWN'.", 0, corrected_df.count())
        else:
            print(f"  Brīdinājums: Kolonna 'CODE' nav pieejama tabulā '{name}'. CODE labošana izlaista.")
            record_issue(results, name, "15C", "CODE_Format_Corrected", "CODE labošana izlaista (trūkst kolonnas).", 0, corrected_df.count())

        # JAUNS: 15.4.1. Identifikatoru labošana (ar prefiksu noņemšanu un formāta validāciju)
        # Saskaņā ar analīzi, "bad-" prefikss netika atrasts sākotnējos datos, bet šis solis joprojām nodrošina vispārīgu formāta validāciju.
        identifiers_to_check = ["ID", "PATIENT", "ENCOUNTER"]
        for id_col in identifiers_to_check:
            if id_col in corrected_df.columns and corrected_df.schema[id_col].dataType == StringType():
                # Pirmkārt, mēģinām noņemt "bad-" prefiksu (vai citus zināmus bojājumus)
                corrected_df = corrected_df.withColumn(
                    id_col,
                    when(
                        col(id_col).cast(StringType()).startswith("bad-"),
                        regexp_replace(col(id_col), "^bad-", "")
                    ).otherwise(col(id_col))
                )
                
                # Pēc tam pārbaudām, vai identifikators atbilst alfanumeriskam formātam ar defisēm (UUID līdzīgs).
                # Regex `^[A-Za-z0-9-]+$` atļauj burtus, ciparus un defises.
                # Ja tas joprojām neatbilst, nomainām uz "UNKNOWN".
                corrected_df = corrected_df.withColumn(
                    id_col,
                    when(
                        (col(id_col).isNotNull()) & (~col(id_col).rlike("^[A-Za-z0-9-]+$")),
                        lit("UNKNOWN")
                    ).otherwise(col(id_col))
                )
                print(f"  Kolonnā '{id_col}': Noņemts 'bad-' prefikss un labotas nederīgas vērtības uz 'UNKNOWN'.")
                record_issue(results, name, "15C", f"{id_col}_Format_Corrected", f"Identifikatora '{id_col}' vērtības labotas (noņemts prefikss, nederīgās uz 'UNKNOWN').", 0, corrected_df.count())
            else:
                print(f"  Brīdinājums: Kolonna '{id_col}' nav pieejama vai nav StringType tabulā '{name}'. Identifikatora labošana izlaista.")
                record_issue(results, name, "15C", f"{id_col}_Format_Corrected", f"Identifikatora '{id_col}' labošana izlaista (trūkst kolonnas vai nav StringType).", 0, corrected_df.count())

        # JAUNS: Vispārēja atstarpju noņemšana virknes kolonnām
        for col_name, dtype in corrected_df.dtypes:
            if dtype == "string":
                corrected_df = corrected_df.withColumn(col_name, trim(col(col_name)))
        print(f"  Visās virknes kolonnās: Noņemtas liekās atstarpes sākumā un beigās.")
        record_issue(results, name, "15C", "Whitespace_Trimmed", "Lieko atstarpju noņemšana virknes kolonnām.", 0, corrected_df.count())


        # 15.5. Starptabulu integritāte: Aizpilda trūkstošos/nepareizos FK, izmantojot "trīs tabulu variantu"
        # ŠIS STARPTABULU INTEGRITĀTES RISINĀJUMS IR NOŅEMTS
        print(f"  Starptabulu integritātes labošanas risinājums ir noņemts no koda. Šis solis (15.5) tiek izlaists tabulai '{name}'.")
        record_issue(results, name, "15C", "Cross_Table_Integrity_Correction_Removed", "Starptabulu integritātes labošanas risinājums noņemts, solis izlaists.", 0, corrected_df.count())


        corrected_df_map[name] = corrected_df

    # --- 16. posms: Laboto datu saglabāšana HDFS ---
    print("\n--- 16. posms: Laboto datu saglabāšana HDFS ---")
    output_base_path = "/dati/synthea_kludainie_dati1_laboti"

    for name, df in corrected_df_map.items():
        output_path = f"{output_base_path}/{name}"
        try:
            df.write.mode("overwrite").option("header", True).csv(output_path)
            print(f"  Tabula '{name}' veiksmīgi saglabāta kā CSV HDFS ceļā: {output_path}")
            record_issue(results, name, "15DS", "Data_Saved_HDFS", f"Labotie dati saglabāti HDFS kā CSV: {output_path}", 0, df.count())
        except Exception as e:
            print(f"  Kļūda saglabājot tabulu '{name}' HDFS: {e}")
            record_issue(results, name, "15DS", "Data_Save_Failed", f"Kļūda saglabājot datus HDFS: {e}", 1, df.count())


    # --- 17. posms: Laboto datu kvalitātes novērtēšana (Post-Correction Quality Assessment) ---
    # Šis posms veic datu kvalitātes pārbaudes PĒC tam, kad dati ir laboti,
    # lai novērtētu labošanas efektivitāti.
    print("\n--- 17. posms: Laboto datu kvalitātes novērtēšana ---")

    for name, df in corrected_df_map.items():
        print(f"\nVeicu PĒC-LABOŠANAS kvalitātes pārbaudes tabulai: '{name}'")
        
        df.cache() # Kešojam DataFrame pirms dārgām darbībām.
        
        try:
            total_rows_corrected = df.count() 
        except Exception as e:
            print(f"  Kļūda, veicot count() labotajai tabulai '{name}': {e}. Izlaižam šo tabulu.")
            record_issue(results, name, "17PC_V", "Volume", "Error counting corrected rows", 0, 0)
            df.unpersist()
            continue

        if total_rows_corrected == 0:
            record_issue(results, name, "17PC_V", "Volume", "Total corrected rows", 0, 0)
            print(f"  Brīdinājums: Labotā tabula '{name}' ir tukša (0 rindas). Tika reģistrēta apjoma problēma. Pārējās pārbaudes izlaistas.")
            df.unpersist()
            continue

        print(f"  Labotā tabula '{name}' satur {total_rows_corrected} rindas un {len(df.columns)} kolonnas.")
        
        # PĒC-LABOŠANAS Pilnīgums (Completeness)
        for col_name, dtype in df.dtypes: # Iterējam pa visām kolonnām
            null_count_pc = df.filter(col(col_name).isNull()).count()
            if null_count_pc > 0:
                record_issue(results, name, "17PC_C", col_name, f"Trūkstošas vērtības (NULL) PĒC-LABOŠANAS", null_count_pc, total_rows_corrected)
            else:
                record_issue(results, name, "17PC_C", col_name, f"Trūkstošas vērtības (NULL) PĒC-LABOŠANAS - OK", 0, total_rows_corrected)

        # PĒC-LABOŠANAS Unikalitāte (Uniqueness)
        # Pārbauda precīzus rindu dublikātus
        dups_pc = total_rows_corrected - df.dropDuplicates().count()
        # JAUNS: Pievienots skaidrojums par unikalitātes rādītāju
        if dups_pc > 0:
            record_issue(results, name, "17PC_U", "Exact_Duplicates", f"Rindu dublikāti PĒC-LABOŠANAS (atlikušie {dups_pc} ieraksti).", dups_pc, total_rows_corrected)
            print(f"  Unikalitāte: Atrasti {dups_pc} precīzi rindu dublikāti PĒC-LABOŠANAS.")
        else:
            record_issue(results, name, "17PC_U", "Exact_Duplicates", "Rindu dublikāti PĒC-LABOŠANAS - OK.", 0, total_rows_corrected)
            print(f"  Unikalitāte: Precīzi rindu dublikāti nav atrasti PĒC-LABOŠANAS.")


        # PĒC-LABOŠANAS Precizitāte (Accuracy)
        if name == "patients" and "GENDER" in df.columns:
            invalid_gender_pc = df.filter(~col("GENDER").isin("M", "F", "U")).count() # 'U' tagad ir derīga vērtība
            record_issue(results, name, "17PC_A", "GENDER", "Nederīgs dzimums PĒC-LABOŠANAS", invalid_gender_pc, total_rows_corrected)
        if name == "patients" and "BIRTHDATE" in df.columns and "DEATHDATE" in df.columns:
            death_before_birth_pc = df.filter(to_date("DEATHDATE") < to_date("BIRTHDATE")).count()
            record_issue(results, name, "17PC_A", "BIRTHDATE/DEATHDATE", "Nāves datums pirms dzimšanas PĒC-LABOŠANAS", death_before_birth_pc, total_rows_corrected)
            
            # JAUNS: PĒC-LABOŠANAS pārbaude datumiem nākotnē
            future_birthdate_pc = df.filter(
                (col("BIRTHDATE").isNotNull()) & 
                (to_date(col("BIRTHDATE")) > current_date()) # Pārliecinās, ka BIRTHDATE tiek pareizi konvertēts uz datumu salīdzināšanai
            ).count()
            record_issue(results, name, "17PC_A", "BIRTHDATE_Future", "Dzimšanas datums nākotnē PĒC-LABOŠANAS", future_birthdate_pc, total_rows_corrected)

            future_deathdate_pc = df.filter(
                (col("DEATHDATE").isNotNull()) & 
                (to_date(col("DEATHDATE")) > current_date()) # Pārliecinās, ka DEATHDATE tiek pareizi konvertēts uz datumu salīdzināšanai
            ).count()
            record_issue(results, name, "17PC_A", "DEATHDATE_Future", "Nāves datums nākotnē PĒC-LABOŠANAS", future_deathdate_pc, total_rows_corrected)


        # PĒC-LABOŠANAS Derīgums (Validity)
        if "CODE" in df.columns:
            # Pārbauda, vai CODE satur tikai ciparus. Ja ir burtu, tas ir nederīgs.
            invalid_code_pc = df.filter(~col("CODE").rlike("^[0-9]+$")).count() # Atļauj tikai ciparus
            record_issue(results, name, "17PC_V", "CODE", "Nederīgs formāts PĒC-LABOŠANAS", invalid_code_pc, total_rows_corrected)
        
        # JAUNS: PĒC-LABOŠANAS SSN un DRIVERS formāta pārbaude
        if name == "patients":
            if "SSN" in df.columns:
                non_numeric_ssn_pc = df.filter(
                    (col("SSN").isNotNull()) & 
                    (~col("SSN").rlike("^[0-9]+$")) & 
                    (col("SSN") != "INVALID_FORMAT") # Izslēdzam jau labotās vērtības
                ).count()
                record_issue(results, name, "17PC_V", "SSN_Format", "SSN satur ne-ciparu rakstzīmes PĒC-LABOŠANAS", non_numeric_ssn_pc, total_rows_corrected)
            
            if "DRIVERS" in df.columns:
                invalid_drivers_pc = df.filter(
                    (col("DRIVERS").isNotNull()) & 
                    (~col("DRIVERS").rlike("^[A-Za-z0-9-]+$")) &
                    (col("DRIVERS") != "INVALID_FORMAT") # Izslēdzam jau labotās vērtības
                ).count()
                record_issue(results, name, "17PC_V", "DRIVERS_Format", "DRIVERS satur nederīgas rakstzīmes PĒC-LABOŠANAS", invalid_drivers_pc, total_rows_corrected)


        # PĒC-LABOŠANAS Savlaicīgums (Timeliness)
        if "START" in df.columns and "STOP" in df.columns:
            # Pārbaudei jāizmanto to_date, ja kolonnas ir StringType pēc labošanas
            wrong_start_stop_pc = df.filter(
                (to_date(col("STOP"), "yyyy-MM-dd").isNotNull()) & 
                (to_date(col("START"), "yyyy-MM-dd").isNotNull()) &
                (to_date(col("STOP"), "yyyy-MM-dd") < to_date(col("START"), "yyyy-MM-dd"))
            ).count()
            record_issue(results, name, "17PC_T", "START/STOP", "STOP pirms START PĒC-LABOŠANAS", wrong_start_stop_pc, total_rows_corrected)

        # PĒC-LABOŠANAS Starptabulu integritāte (Cross-table integrity)
        if determined_domain == "Veselības aprūpe":
            if name in reference_rules:
                for fk_col, ref_table_name, pk_col in reference_rules[name]:
                    if ref_table_name in corrected_df_map: # Izmantojam corrected_df_map, lai pārbaudītu pret labotajām tabulām
                        ref_df_pc = corrected_df_map[ref_table_name]
                        if fk_col in df.columns and pk_col in ref_df_pc.columns:
                            ref_pk_values_pc = ref_df_pc.select(pk_col).rdd.flatMap(lambda x: x).collect()
                            
                            if not ref_pk_values_pc: 
                                print(f"  Brīdinājums: Atsauces tabulas '{ref_table_name}' primārās atslēgas kolonna '{pk_col}' ir tukša vai satur tikai NULL vērtības PĒC-LABOŠANAS. Atsauces integritātes pārbaude var būt neprecīza.")
                                record_issue(results, name, "17PC_RI", fk_col, f"Atsauces tabulas '{ref_table_name}' PK ir tukša PĒC-LABOŠANAS. Pārbaude var būt neprecīza.", 1, total_rows_corrected)
                                continue 
                            
                            ref_pk_broadcast_pc = spark.sparkContext.broadcast(set(ref_pk_values_pc))

                            invalid_fk_count_pc = df.filter(
                                (col(fk_col).isNotNull()) & 
                                (~lower(col(fk_col)).isin(ref_pk_broadcast_pc.value)) 
                            ).count()
                            
                            if invalid_fk_count_pc > 0:
                                issue_desc = f"Atsauces integritātes pārkāpums PĒC-LABOŠANAS: Kolonna '{fk_col}' satur {invalid_fk_count_pc} vērtības, kas neeksistē '{ref_table_name}.{pk_col}'."
                                record_issue(results, name, "17PC_RI", fk_col, issue_desc, invalid_fk_count_pc, total_rows_corrected)
                                print(f"  Piemēri neatbilstošām '{fk_col}' vērtībām (PĒC-LABOŠANAS):")
                                df.filter(
                                    (col(fk_col).isNotNull()) & 
                                    (~lower(col(fk_col)).isin(ref_pk_broadcast_pc.value))
                                ).select(fk_col).limit(5).show(truncate=False)
                            else:
                                record_issue(results, name, "17PC_RI", fk_col, f"Atsauces integritāte OK PĒC-LABOŠANAS uz '{ref_table_name}.{pk_col}'.", 0, total_rows_corrected)
                            
                        else:
                            record_issue(results, name, "17PC_RI", fk_col, f"Atsauces integritātes pārbaude netika veikta (trūkst kolonnas) PĒC-LABOŠANAS", 1, 1)
                            print(f"  Brīdinājums: Nevar veikt atsauces integritātes pārbaudi kolonnai '{fk_col}' tabulā '{name}' (trūkst kolonnas) PĒC-LABOŠANAS.")
                    else:
                        record_issue(results, name, "17PC_RI", fk_col, f"Atsauces tabula '{ref_table_name}' nav ielādēta PĒC-LABOŠANAS", 1, 1)
                        print(f"  Brīdinājums: Atsauces tabula '{ref_table_name}' nav ielādēta PĒC-LABOŠANAS. Nevar veikt atsauces integritātes pārbaudi.")
            else:
                record_issue(results, name, "17PC_RI", "Referential_Integrity_Checks", "Atsauces integritātes noteikumi nav definēti PĒC-LABOŠANAS šai tabulai", 0, total_rows_corrected)
                print(f"  Atsauces integritāte: Noteikumi nav definēti PĒC-LABOŠANAS šai tabulai.")
        else:
            record_issue(results, name, "17PC_RI", "Domain_Specific_Referential_Integrity_Checks", "Domēnspecifiskās atsauces integritātes pārbaudes izlaistas (nav 'Veselības aprūpe' domēns) PĒC-LABOŠANAS", 0, total_rows_corrected)
            print(f"  Starptabulu integritātes pārbaudes netika veiktas, jo domēns nav 'Veselības aprūpe' PĒC-LABOŠANAS.")

        df.unpersist() 
        print(f"  Labotās tabulas '{name}' kešatmiņa atbrīvota.")


    # --- 14. posms: Kvalitātes indikatoru aprēķins un atskaišu veidošana ---
    print("\n--- 14. posms: Kvalitātes indikatoru aprēķins un atskaišu veidošana ---")

    if not results:
        print("Nav atrasti datu kvalitātes rezultāti, ko apkopot.")
    else:
        # Pārveidojam rezultātus par Pandas DataFrame vieglākai apstrādei
        results_df = pd.DataFrame(results)

        print("\nVizualizācijas kopsavilkums:")
        print("----------------------------------------------------------------------------------------------------")

        # Definējam saīsinājumu kartēšanu uz pilniem nosaukumiem
        quality_dimension_map = {
            "5DD": "Datu Domēna Noteikšana",
            "5MD": "Metadatu Definīcija",
            "5SV": "Shēmas Validācija",
            "6V": "Apjoma Pārbaudes",
            "7S": "Sintaktiskā Validācija",
            "8BL": "Biznesa Loģikas Validācija",
            "8RI": "Atsauces Integritāte (Pirms Labošanas)", # Precizēts nosaukums
            "9S": "Semantiskā/Ontoloģiskā Validācija",
            "10CL": "Klīniskās Loģikas Pārbaudes",
            "11FI": "Trūkstošo Lauku Aizpildīšana (Noņemts/Izlaists)", # Atjaunots, lai atspoguļotu izmaiņas
            "12C": "Pilnīgums (Pirms Labošanas)", # Precizēts nosaukums
            "12T": "Savlaicīgums (Pirms Labošanas)", # Precizēts nosaukums
            "15C": "Datu Labošana",
            "15DS": "Datu Saglabāšana HDFS", # Atjaunināts
            "17PC_C": "Pilnīgums (Pēc Labošanas)", # Jauns
            "17PC_U": "Unikalitāte (Pēc Labošanas)", # Jauns
            "17PC_A": "Precizitāte (Pēc Labošanas)", # Jauns
            "17PC_V": "Derīgums/Apjoms (Pēc Labošanas)", # Apvienots, jo "17PC_V" tika lietots abiem
            "17PC_T": "Savlaicīgums (Pēc Labošanas)", # Jauns
            "17PC_RI": "Atsauces Integritāte (Pēc Labošanas)", # Jauns
        }

        # Problēmu kopsavilkums pa kvalitātes dimensijām
        if not results_df.empty:
            # Izmantojam .copy() lai izvairītos no SettingWithCopyWarning
            issues_by_dimension_summary = results_df.groupby('quality_dimension')['error_count'].sum().reset_index().copy()
            # Kartējam saīsinājumus uz pilniem nosaukumiem
            issues_by_dimension_summary['quality_dimension'] = issues_by_dimension_summary['quality_dimension'].map(quality_dimension_map).fillna(issues_by_dimension_summary['quality_dimension'])
            issues_by_dimension_summary.columns = ['Kvalitātes Dimensija', 'Kļūdu Skaits']
            print("\nProblēmu kopsavilkums pa kvalitātes dimensijām:")
            print(issues_by_dimension_summary.to_string(index=False))
            print("\n")
        else:
            print("\nNav datu, lai apkopotu problēmas pa kvalitātes dimensijām.")

        # Problēmu kopsavilkums pa tabulām
        if not results_df.empty:
            issues_by_table_summary = results_df.groupby('table_name')['error_count'].sum().reset_index()
            issues_by_table_summary.columns = ['Tabulas Nosaukums', 'Kļūdu Skaits']
            print("Problēmu kopsavilkums pa tabulām:")
            print(issues_by_table_summary.to_string(index=False))
            print("\n")
        else:
            print("Nav datu, lai apkopotu problēmas pa tabulām.")

        # Top 10 kolonnas ar visvairāk problēmām
        if not results_df.empty:
            top_columns_with_issues = results_df.groupby(['table_name', 'rule_name'])['error_count'].sum().reset_index()
            top_columns_with_issues = top_columns_with_issues.sort_values(by='error_count', ascending=False).head(10)
            top_columns_with_issues.columns = ['Tabula', 'Kolonna/Noteikums', 'Kļūdu Skaits'] # Mainīts kolonnas nosaukums
            print("Top 10 kolonnas/noteikumi ar visvairāk problēmām:")
            print(top_columns_with_issues.to_string(index=False))
            print("\n")
        else:
            print("Nav datu, lai apkopotu top kolonnas ar problēmām.")

        print("----------------------------------------------------------------------------------------------------")
        print("Piezīme: Šis ir teksta bāzes vizualizācijas kopsavilkums. Lai iegūtu interaktīvas grafiskās vizualizācijas (piemēram, stabiņu diagrammas, sektoru diagrammas),")
        print("jūs varat eksportēt 'results' JSON datus uz failu un izmantot ārējus rīkus, piemēram, Python ar Matplotlib/Seaborn, Power BI, Tableau vai citas datu vizualizācijas platformas.")
        print("Dati JSON formātā ir pieejami iepriekšējā posmā.")

        # Jauns: JSON izraksts
        print("\n--- Datu kvalitātes rezultāti JSON formātā ---")
        print(json.dumps(results, indent=2, default=str)) # default=str nodrošina, ka datuma/laika objekti tiek serializēti kā virknes
        print("--------------------------------------------------")


finally:
    # Atvienošanās no Spark sesijas - tiek izpildīts vienmēr
    if 'spark' in locals() and spark: # Pārbauda, vai Spark sesija vispār ir izveidota
        print("\nIzslēdzu Spark sesiju...")
        spark.stop()
        print("Spark sesija izslēgta.")