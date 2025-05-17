import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum as spark_sum, min, max, lit, monotonically_increasing_id # rlike nav jāimportē

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


def record_issue(results, table, check_type, column, desc, affected, total):
    # Noņemts nosacījums 'if affected > 0:' - vienmēr reģistrē rezultātu, lai ir konsekvence ar kļūdu injicēto versiju
    # Uzlabots procentu aprēķins, apstrādājot total kā None vai 0
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
# appName:	Lietotnes nosaukums (redzams Spark UI)
# master:	Spark klastera adrese (spark-master:7077)
# executor.cores:	1 CPU kodols izpildītājam
# executor.memory:	512MB atmiņas izpildītājam
# fs.defaultFS:	HDFS adrese (hdfs://namenode:9000)
# sql.debug.maxToStringFields:	Rāda 100 laukus atkļūdošanai
# sql.legacy.timeParserPolicy:	Atbalsta vecākus laika formātus
# scheduler.listenerbus.capacity:	Notikumu buferis (20 000 ieraksti)
# sql.execution.arrow.enabled:	Paātrina darbu ar Pandas
# dynamicAllocation.enabled:	Automātiski regulē izpildītājus
# Pēdējā rinda: .getOrCreate(): izveido vai atgriež esošo sesiju

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
    .getOrCreate()

# 3. Nolasa daļēji bojātus datus no HDFS dažādi formāti, tostarp delta kā parkets, tikai bez ACID, jo Delta-Lake radīja papildus sarežģītības
base_path = "/dati/synthea_kludainie_dati1" # Ceļš uz HDFS
valid_formats = ["csv", "json", "parquet", "avro", "delta"]
df_map = {}

# Lieto Hadoop datņu sistēmas FileSystem API izmantojot JVM
try: # Pievienots try/except ap HDFS piekļuvi
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path

    # Pārbauda, vai base_path eksistē un ir attiecīgās direktorijs
    base_hdfs_path_obj = Path(base_path)
    if not fs.exists(base_hdfs_path_obj):
        print(f"Kļūda: HDFS bāzes ceļš {base_path} neeksistē.")
    elif not fs.isDirectory(base_hdfs_path_obj):
        print(f"Kļūda: HDFS bāzes ceļš {base_path} nav direktorijas.")
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
                            if fmt == "csv":
                                df = spark.read.option("header", True).csv(fmt_dir)
                            elif fmt == "json":
                                df = spark.read.option("header", True).json(fmt_dir)
                            elif fmt == "parquet":
                                df = spark.read.parquet(fmt_dir)
                            elif fmt == "avro":
                                df = spark.read.format("avro").load(fmt_dir)
                            elif fmt == "delta":
                                df = spark.read.format("parquet").load(fmt_dir)
                            df_map[table_name] = df
                            # print(f"Ielādēta tabula '{table_name}' no formāta '{fmt}' ceļā {fmt_dir}") # Ja ir vēlme, var izprintēt debug par ceļu uz failiem
                            break
                        except Exception as read_e:
                            print(f"Kļūda lasot datus no {fmt_dir} ({fmt}): {read_e}")

except Exception as hdfs_e:
    print(f"Kļūda piekļūstot HDFS bāzes ceļam {base_path} vai apstrādājot apakšdirektorijas: {hdfs_e}")


# 4. Datu kvalitātes pārbaudes
results = []
for name, df in df_map.items():
    # Labojums: Pārbauda, vai daturāmis (DataFrame) ir tukšs, izmantojot count() == 0
    if df.count() == 0:
        # Reģistrē Volume = 0, ja datne ir tukša
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
if "medications" in df_map and "encounters" in df_map:
     m = df_map["medications"].join(df_map["encounters"], df_map["medications"]["ENCOUNTER"] == df_map["encounters"]["ID"], "left_anti").count()
     record_issue(results, "medications", "Starptabulu integritāte", "medications.ENCOUNTER→encounters.ID", "Nepastāv saistīts encounters ieraksts", m, df_map["medications"].count())

# 5. Ieraksta datus datubžē PostgreSQL
conn = get_pg_connection()
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS error_synthea_data_quality_2025 (
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

# Iztukšo tabulu un atiestata SERIAL identifikatoru uz 1, ja dati tiek dzēsti jeb izmantota komanda (truncate)
cur.execute("TRUNCATE TABLE error_synthea_data_quality_2025 RESTART IDENTITY;")
conn.commit() # Apstiprina iztukšošanas operāciju

for r in results:
    cur.execute(
        """
        INSERT INTO error_synthea_data_quality_2025
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
cur.close()
conn.close()
print("Datu kvalitātes novērtējums ir pabeigts un ierakstīti tabulā error_synthea_data_quality_2025.")