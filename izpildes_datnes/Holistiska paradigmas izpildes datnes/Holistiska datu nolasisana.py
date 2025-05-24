import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum as spark_sum, min, max, lit # rlike nav jāimportē, jo ir Column metode

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
    Reģistrē datu kvalitātes problēmu.
    Noņemts nosacījums 'if affected > 0' - vienmēr reģistrē rezultātu, lai nodrošinātu konsekvenci.
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
spark = SparkSession.builder \
    .appName("kludaino_SyntheaVeselibasDatuKvalitatesNovertesanaHDFS") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "512m") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.debug.maxToStringFields", 100) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.scheduler.listenerbus.eventqueue.capacity", "20000") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.pyspark.python", "/opt/venv/bin/python") \
    .config("spark.pyspark.driver.python", "/opt/venv/bin/python") \
    .getOrCreate()

# Nodrošinām, ka Spark sesija tiks pārtraukta neatkarīgi no kļūdām
try:
    # 3. Datu nolasīšana no HDFS (dažādi formāti, tostarp delta kā parquet)
    base_path = "/dati/synthea_kludainie_dati1_laboti" 
    valid_formats = ["csv", "json", "parquet", "avro", "delta"]
    df_map = {}

    # Izmanto Hadoop datņu sistēmas FileSystem API, izmantojot JVM
    try:
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        Path = spark._jvm.org.apache.hadoop.fs.Path

        # Pārbaudām, vai base_path eksistē un ir direktorijs
        base_hdfs_path_obj = Path(base_path)
        if not fs.exists(base_hdfs_path_obj):
            print(f"Kļūda: HDFS bāses ceļš {base_path} neeksistē.")
        elif not fs.isDirectory(base_hdfs_path_obj):
            print(f"Kļūda: HDFS bāses ceļš {base_path} nav direktorijs.")
        else:
            print(f"\nSāku nolasīt datus no HDFS bāzes ceļa: {base_path}")
            for table_status in fs.listStatus(base_hdfs_path_obj):
                if table_status.isDirectory():
                    table_dir = table_status.getPath().toString()
                    table_name = os.path.basename(table_dir).replace('_corrupted','')
                    print(f"\n  Atrasta tabulas direktorija: '{table_name}' ceļā {table_dir}")
                    
                    found_data_file = False
                    # Iterē cauri failiem un apakšdirektorijām tabulas direktorijā
                    for file_status in fs.listStatus(Path(table_dir)):
                        file_path = file_status.getPath().toString()
                        file_name = os.path.basename(file_path)
                        
                        # Izlaižam _SUCCESS failus un citas ne-datu failus, kā arī apakšdirektorijas
                        if file_name.startswith("_") or file_status.isDirectory():
                            continue

                        # Nosakām formātu pēc faila paplašinājuma
                        file_extension = file_name.split('.')[-1].lower()
                        
                        if file_extension in valid_formats:
                            print(f"    Mēģinu ielādēt tabulu '{table_name}' no faila ar formātu '.{file_extension}' ceļā {file_path}...")
                            try:
                                df = None # Inicē df
                                # Spark automātiski nolasīs visus "part" failus norādītajā direktorijā
                                if file_extension == "csv":
                                    df = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(table_dir)
                                elif file_extension == "json":
                                    df = spark.read.option("header", True).option("mode", "PERMISSIVE").json(table_dir)
                                elif file_extension == "parquet":
                                    df = spark.read.parquet(table_dir)
                                elif file_extension == "avro":
                                    df = spark.read.format("avro").load(table_dir)
                                elif file_extension == "delta":
                                    df = spark.read.format("parquet").load(table_dir) # Apstrādā delta mapi kā parquet snapshot
                                
                                # Pārbaudām, vai DataFrame ir patiešām ielādēts un lietojams
                                if df is not None:
                                    row_count = df.count()
                                    df_map[table_name] = df
                                    print(f"      Veiksmīgi ielādēta tabula '{table_name}' ar {row_count} rindām.")
                                    found_data_file = True
                                    break # Atrasts derīgs datu fails un ielādēta tabula, pāriet pie nākamās tabulas
                                else:
                                    print(f"      Brīdinājums: Neizdevās ielādēt DataFrame no {table_dir} (formāts: {file_extension}).")

                            except Exception as read_e:
                                print(f"      Kļūda lasot datus no {table_dir} (formāts: {file_extension}): {read_e}")
                                # Turpinām ar nākamo failu/formātu, ja šis neizdevās
                                
                    if not found_data_file:
                        print(f"  Brīdinājums: Tabulai '{table_name}' netika atrasts neviens atbalstīts datu fails ({', '.join(valid_formats)}) vai neizdevās ielādēt datus.")


    except Exception as hdfs_e:
        print(f"Kļūda piekļūstot HDFS bāzes ceļam {base_path} vai apstrādājot apakšdirektorijus: {hdfs_e}")


    # 4. Datu kvalitātes pārbaudes
    results = []
    # Pārbaudām, vai df_map ir tukšs. Ja nav ielādētas tabulas, nav ko apstrādāt.
    if not df_map:
        print("\nBrīdinājums: Nav ielādētas tabulas no HDFS. Datu kvalitātes pārbaudes netiks veiktas.")
    else:
        for name, df in df_map.items():
            # Pārbaudām, vai DataFrame ir tukšs
            if df.count() == 0:
                record_issue(results, name, "6V", "Volume", "Total rows", 0, 0)
                continue

            total = df.count()
            cols = [c.upper() for c in df.columns]

            # Pilnīgums (Completeness)
            for c in cols:
                nulls = df.filter(col(c).isNull() | (col(c) == "")).count()
                record_issue(results, name, "Pilnīgums (Completeness)", c, "Null vai tukšas vērtības", nulls, total)

            # Unikalitāte (Uniqueness)
            dups = total - df.dropDuplicates().count()
            record_issue(results, name, "Unikalitāte (Uniqueness)", None, "Rindu dublikāti", dups, total)

            # Precizitāte (Accuracy)
            if "GENDER" in cols:
                invalid_gender = df.filter(~col("GENDER").isin("M", "F", "U")).count()
                record_issue(results, name, "Precizitāte (Accuracy)", "GENDER", "Nederīgs dzimums", invalid_gender, total)
            if "BIRTHDATE" in cols:
                invalid_birth = df.withColumn("dt", to_date("BIRTHDATE")).filter(col("dt").isNull()).count()
                record_issue(results, name, "Precizitāte (Accuracy)", "BIRTHDATE", "Nederīgs datums", invalid_birth, total)

            # Savlaicīgums (Timeliness)
            if "DEATHDATE" in cols and "BIRTHDATE" in cols:
                bad_dates = df.filter(to_date("DEATHDATE") < to_date("BIRTHDATE")).count()
                record_issue(results, name, "Savlaicīgums (Timeliness)", "BIRTHDATE/DEATHDATE", "Nāves datums pirms dzimšanas", bad_dates, total)
            if "START" in cols and "STOP" in cols:
                wrong = df.filter(to_date("STOP") < to_date("START")).count()
                record_issue(results, name, "Savlaicīgums (Timeliness)", "START/STOP", "STOP pirms START", wrong, total)

            # Derīgums (Validity)
            if "CODE" in cols:
                # rlike ir Column metode, nav jāimportē atsevišķi
                invalid_code = df.filter(~col("CODE").rlike("^[A-Za-z0-9\.\-]+$")).count()
                record_issue(results, name, "Derīgums (Validity)", "CODE", "Nederīgs formāts", invalid_code, total)

        # Starptabulu integritāte (Cross-table integrity)
        if "encounters" in df_map and "patients" in df_map:
            m = df_map["encounters"].join(df_map["patients"], df_map["encounters"]["PATIENT"] == df_map["patients"]["ID"], "left_anti").count()
            record_issue(results, "encounters", "Starptabulu integritāte", "PATIENT→patients.ID", "Nepastāv saistīts pacients", m, df_map["encounters"].count())
        if "conditions" in df_map and "encounters" in df_map:
            m = df_map["conditions"].join(df_map["encounters"], df_map["conditions"]["ENCOUNTER"] == df_map["encounters"]["ID"], "left_anti").count()
            record_issue(results, "conditions", "Starptabulu integritāte", "ENCOUNTER→encounters.ID", "Nepastāv saistīts encounters ieraksts", m, df_map["conditions"].count())
        if "medications" in df_map and "patients" in df_map:
            m = df_map["medications"].join(df_map["patients"], df_map["medications"]["PATIENT"] == df_map["patients"]["ID"], "left_anti").count()
            record_issue(results, "medications", "Starptabulu integritāte", "PATIENT→patients.ID", "Nepastāv saistīts pacients", m, df_map["medications"].count())
        # Pievienojam otru starptabulu pārbaudi medications tabulai, kas bija iepriekšējā kodā
        if "medications" in df_map and "encounters" in df_map:
            m = df_map["medications"].join(df_map["encounters"], df_map["medications"]["ENCOUNTER"] == df_map["encounters"]["ID"], "left_anti").count()
            record_issue(results, "medications", "Starptabulu integritāte", "medications.ENCOUNTER→encounters.ID", "Nepastāv saistīts encounters ieraksts", m, df_map["medications"].count())

    # 5. Ieraksta datus datubāzē PostgreSQL
    conn = None # Inicējam savienojumu kā None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS synthea_kludainie_dati1_laboti (
            id SERIAL PRIMARY KEY,
            table_name TEXT,
            quality_dimension TEXT,
            rule_name TEXT,
            issue_description TEXT,
            error_count INTEGER,
            total_count INTEGER,
            error_percentage FLOAT,
            checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(table_name, quality_dimension, rule_name, issue_description,
                    error_count, total_count, error_percentage)
        );
        """)
        conn.commit() # Apstiprina iztukšošanas operāciju

        # Iztukšo tabulu un atiestata SERIAL identifikatoru uz 1
        cur.execute("TRUNCATE TABLE synthea_kludainie_dati1_laboti RESTART IDENTITY;")
        conn.commit() # Apstiprina iztukšošanas operāciju

        if results: # Ierakstām tikai tad, ja ir rezultāti
            for r in results:
                cur.execute(
                    """
                    INSERT INTO synthea_kludainie_dati1_laboti
                    (table_name, quality_dimension, rule_name, issue_description,
                     error_count, total_count, error_percentage)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING;
                    """, (
                        r["table_name"], r["quality_dimension"], r["rule_name"],
                        r["issue_description"], r["error_count"], r["total_count"],
                        r["error_percentage"]
                    )
                )
            conn.commit()
            print("Datu kvalitātes novērtējums pabeigts un ierakstīti tabulā synthea_kludainie_dati1_laboti.")
        else:
            print("Nav datu kvalitātes rezultātu, ko ierakstīt datubāzē.")

    except Exception as db_e:
        print(f"Kļūda, ierakstot datus PostgreSQL datubāzē: {db_e}")
        if conn:
            conn.rollback() # Atceļam transakciju kļūdas gadījumā
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

finally:
    # Atvienošanās no Spark sesijas - tiek izpildīts vienmēr
    if spark: # Pārbauda, vai Spark sesija vispār ir izveidota
        print("\nIzslēdzu Spark sesiju...")
        spark.stop()
        print("Spark sesija izslēgta.")
