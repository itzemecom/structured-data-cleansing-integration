import os
from datetime import datetime
import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_sub, rand, when, concat, lit, date_format, sum as spark_sum, min, max, count, lower, upper, trim, regexp_replace, regexp_extract, current_date, datediff, months_between, floor, coalesce, length
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, DateType, TimestampType, IntegerType, BooleanType
import json
import pandas as pd

# 1. Spark sesijas inicializācija
spark = SparkSession.builder \
    .appName("SyntheaDatuIntegritatesLabosanaUnAtskaite") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.driver.maxResultSize", "2g") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .config("spark.sql.debug.maxToStringFields", 100) \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.scheduler.listenerbus.eventqueue.capacity", "20000") \
    .config("spark.pyspark.python", "/opt/venv/bin/python") \
    .config("spark.pyspark.driver.python", "/opt/venv/bin/python") \
    .config("spark.sql.codegen.wholeStage", "false") \
    .config("spark.sql.codegen.maxFields", "100") \
    .getOrCreate()

# Iestatām Spark žurnālēšanas līmeni uz ERROR, lai samazinātu konsoles izvades apjomu.
spark.sparkContext.setLogLevel("ERROR")

# Iestatām checkpoint direktoriju
# Pārliecinieties, ka šis ceļš ir pieejams un rakstāms no Spark darbiniekiem
spark.sparkContext.setCheckpointDir("hdfs:///tmp/spark-checkpoints")

# Nodrošinām, ka Spark sesija tiks pārtraukta neatkarīgi no kļūdām
try:
    # 2. Palīgfunkcijas tukšo/nederīgo vērtību noteikšanai (izmanto gan labošanā, gan atskaitē)
    def is_effectively_empty_string(col_ref):
        """
        Pārbauda, vai virknes kolonna ir NULL, tukša, satur tikai atstarpes,
        vai satur specifiskas aizstājējvērtības.
        """
        return (
            col_ref.isNull() |                  # NULL vērtības
            (trim(col_ref) == "") |             # Tukšas virknes vai virkmes ar tikai atstarpēm
            (lower(trim(col_ref)) == "0000") |  # Specifisks aizstājējs "0000"
            (lower(trim(col_ref)) == "unknown") | # Specifisks aizstājējs "UNKNOWN"
            (lower(trim(col_ref)) == "n/a")     # Specifisks aizstājējs "N/A"
        )

    def is_invalid_uuid_value(col_ref):
        """
        Pārbauda, vai kolonna, kurai jābūt UUID, ir nederīga.
        Ietver tukšu/aizstājēju pārbaudes, UUID formāta regex un garuma pārbaudi.
        """
        uuid_regex = "^[A-Fa-f0-9]{8}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{4}-[A-Fa-f0-9]{12}$"
        return (
            col_ref.isNull() |                  # NULL vērtības
            (trim(col_ref) == "") |             # Tukšas virknes vai virkmes ar tikai atstarpēm
            (lower(trim(col_ref)) == "0000") |  # Specifisks aizstājējs "0000"
            (lower(trim(col_ref)) == "unknown") | # Specifisks aizstājējs "UNKNOWN"
            (lower(trim(col_ref)) == "n/a") |   # Specifisks aizstājējs "N/A"
            (length(col_ref) != 36) |           # UUID jābūt 36 rakstzīmēm garam
            (~col_ref.rlike(uuid_regex))        # Vai neatbilst UUID formātam
        )


    # 3. Datu nolasīšana no HDFS (sākotnējie, "netīrie" dati labošanai)
    base_path_original_data = "/dati/synthea_kludainie_dati1"
    valid_formats = ["csv", "json", "parquet", "avro", "delta"]
    df_map_original = {} # Vārdnīca sākotnējiem datiem

    print(f"\nSāku nolasīt SĀKOTNĒJOS datus no HDFS ceļa: {base_path_original_data} labošanai.")

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path

    base_hdfs_path_obj_original = Path(base_path_original_data)

    if not fs.exists(base_hdfs_path_obj_original):
        print(f"Kļūda: HDFS bāses ceļš {base_path_original_data} neeksistē.")
    elif not fs.isDirectory(base_hdfs_path_obj_original):
        print(f"Kļūda: HDFS bāses ceļš {base_path_original_data} nav direktorijas.")
    else:
        for table_status in fs.listStatus(base_hdfs_path_obj_original):
            if table_status.isDirectory():
                table_dir = table_status.getPath().toString()
                table_name = os.path.basename(table_dir).replace('_corrupted','')
                print(f"  Atrasta tabulas direktorija: '{table_name}' ceļā {table_dir}")

                found_data_file = False
                for fmt_status in fs.listStatus(Path(table_dir)):
                    fmt_dir = fmt_status.getPath().toString()
                    fmt = os.path.basename(fmt_dir).lower()
                    if fmt in valid_formats:
                        try:
                            print(f"    Mēģinu ielādēt tabulu '{table_name}' no formāta '{fmt}' ceļā {fmt_dir}...")
                            df = None
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

                            if df is not None:
                                row_count = df.count()
                                if row_count > 0:
                                    df_map_original[table_name] = df
                                    print(f"      Veiksmīgi ielādēta tabula '{table_name}' ar {row_count} rindām.")
                                    found_data_file = True
                                    break
                                else:
                                    print(f"      Brīdinājums: Ielādētā tabula '{table_name}' no {fmt_dir} ir tukša (0 rindas).")
                            else:
                                print(f"      Brīdinājums: Neizdevās ielādēt DataFrame no {fmt_dir} ({fmt}).")

                        except Exception as read_e:
                            print(f"      Kļūda lasot datus no {fmt_dir} ({fmt}): {read_e}")
                if not found_data_file:
                    print(f"  Brīdinājums: Tabulai '{table_name}' netika atrasts neviens atbalstīts datu fails ({', '.join(valid_formats)}) vai neizdevās ielādēt datus.")


    # Definējam primārās atslēgas (PK) un atsauces integritātes noteikumus
    primary_keys = {
        "patients": ["Id"],
        "encounters": ["ID"],
        "conditions": ["PATIENT", "CODE", "START"],
        "medications": ["PATIENT", "CODE", "START"],
        "allergies": ["PATIENT", "CODE", "START"],
    }

    reference_rules = {
        "allergies": [("PATIENT", "patients", "Id"), ("ENCOUNTER", "encounters", "ID")],
        "conditions": [("PATIENT", "patients", "Id"), ("ENCOUNTER", "encounters", "ID")],
        "encounters": [("PATIENT", "patients", "Id")],
        "medications": [("PATIENT", "patients", "Id"), ("ENCOUNTER", "encounters", "ID")],
    }

    # 4. Datu labošana (tikai integritātes labojumi ar jaunajiem nosacījumiem)
    print("\n--- Sākam datu integritātes labošanu ---")
    corrected_df_map = {}
    results_fix = [] # Atsevišķs rezultātu saraksts labošanas posmam

    if not df_map_original:
        print("\nBrīdinājums: Nav ielādētas sākotnējās tabulas labošanai. Labošana netiks veikta.")
    else:
        # Pārveidota identifiers_to_check struktūra, lai novērstu atkārtojumus
        identifiers_to_check = {
            "patients": ["Id"],
            "encounters": ["ID"],
            "allergies": ["PATIENT", "ENCOUNTER"],
            "conditions": ["PATIENT", "ENCOUNTER"],
            "medications": ["PATIENT", "ENCOUNTER"]
        }

        for name, df_original in df_map_original.items():
            print(f"\nVeicu datu integritātes labošanu tabulai: '{name}'")
            corrected_df = df_original
            total_rows_before_correction = df_original.count()

            if name in identifiers_to_check:
                for id_col in identifiers_to_check[name]:
                    if id_col in corrected_df.columns and corrected_df.schema[id_col].dataType == StringType():
                        # Saskaitām nederīgās ID vērtības pirms labošanas
                        # is_invalid_uuid_value jau aptver tukšumus un nederīgu UUID formātu
                        initial_invalid_id_count = corrected_df.filter(is_invalid_uuid_value(col(id_col))).count()

                        # Labojam: ja vērtība nav derīgs UUID (t.sk. tukša), tad to nomaina uz "UNKNOWN".
                        corrected_df = corrected_df.withColumn(
                            id_col,
                            when(
                                is_invalid_uuid_value(col(id_col)),
                                lit("UNKNOWN")
                            ).otherwise(col(id_col))
                        )

                        if initial_invalid_id_count > 0:
                            print(f"  Tabula '{name}', kolonnā '{id_col}': {initial_invalid_id_count} nederīgas UUID (t.sk. tukšas) vērtības labotas uz 'UNKNOWN'.")
                            results_fix.append({
                                "table_name": name,
                                "quality_dimension": "Integ_Fix",
                                "rule_name": f"{id_col}_Format_Corrected",
                                "issue_description": f"{initial_invalid_id_count} identifikatora '{id_col}' nederīgas UUID/tukšas vērtības labotas uz 'UNKNOWN'.",
                                "error_count": initial_invalid_id_count,
                                "total_count": total_rows_before_correction,
                                "error_percentage": round((initial_invalid_id_count / total_rows_before_correction) * 100, 2) if total_rows_before_correction > 0 else 0.0
                            })
                        else:
                            print(f"  Tabula '{name}', kolonnā '{id_col}': Nav atrasti labojami nederīgi UUID vai tukši identifikatori.")
            
            # Pēc identifikatoru labošanas, veicam checkpoint, lai "salauztu" izpildes grafu
            corrected_df = corrected_df.checkpoint()
            print(f"  Checkpoint veikts pēc identifikatoru labošanas tabulai '{name}'.")


            # Starptabulu integritāte: Aizpilda trūkstošos/nepareizos FK
            if name in reference_rules:
                for fk_col, ref_table_name, pk_col in reference_rules[name]:
                    if ref_table_name in df_map_original and fk_col in corrected_df.columns and pk_col in df_map_original[ref_table_name].columns:
                        
                        # Iegūstam atsauces PK vērtības no SĀKOTNĒJĀM tabulām
                        # Svarīgi: te izmantojam df_map_original[ref_table_name] nevis corrected_df_map,
                        # jo labojam datus attiecībā pret oriģinālajiem PK.
                        ref_pk_values = df_map_original[ref_table_name].select(pk_col).rdd.flatMap(lambda x: x).collect()

                        if not ref_pk_values:
                            print(f"  Brīdinājums: Atsauces tabulas '{ref_table_name}' primārās atslēgas kolonna '{pk_col}' ir tukša. Atsauces integritātes labošana var būt neprecīza.")
                            continue

                        ref_pk_broadcast = spark.sparkContext.broadcast(set(ref_pk_values))

                        # Pirms labošanas: saskaitām, cik ierakstu NEatbilst atsauces tabulai (pēc tam, kad nederīgie ir uz 'UNKNOWN')
                        # Mēs uzskatām, ka 'UNKNOWN' arī nav atrasts atsaucē, ja vien tas nav īsts PK.
                        # Tāpēc neieskaitām "UNKNOWN" un tukšās vērtības, jo tās jau ir labotas.
                        initial_fk_issues_count = corrected_df.filter(
                            (~lower(col(fk_col)).isin(ref_pk_broadcast.value)) & # Nav atrasts atsaucē
                            (~is_effectively_empty_string(col(fk_col))) & # Nav tukšs/UNKNOWN/N/A
                            (lower(col(fk_col)) != "unknown") # Nav "UNKNOWN"
                        ).count()

                        # Patient FK gadījumā ar trīs tabulu variantu (tagad tikai, ja PATIENT vērtība IR "UNKNOWN")
                        if name in ["conditions", "allergies", "medications"] and fk_col == "PATIENT" and \
                           "ENCOUNTER" in corrected_df.columns and "encounters" in df_map_original and "patients" in df_map_original:

                            encounters_df_for_join = df_map_original["encounters"].select(
                                col("ID").alias("enc_id_join"),
                                col("PATIENT").alias("enc_patient_id_derived")
                            )
                            patients_pk_broadcast_for_derived = spark.sparkContext.broadcast(set(df_map_original["patients"].select("Id").rdd.flatMap(lambda x: x).collect()))

                            # Pievienojam pagaidu kolonnu ar atvasinātu pacienta ID
                            temp_df_with_derived = corrected_df.alias("current_table") \
                                .join(
                                    encounters_df_for_join.alias("enc"),
                                    (lower(col("current_table.ENCOUNTER")) == lower(col("enc.enc_id_join"))),
                                    "left_outer"
                                ) \
                                .withColumn(
                                    "temp_derived_patient_id_candidate",
                                    when(
                                        (lower(col("current_table." + fk_col)) == "unknown") & # Tikai tad, ja pašreizējais FK ir "UNKNOWN"
                                        (col("enc.enc_patient_id_derived").isNotNull()) &
                                        (lower(col("enc.enc_patient_id_derived")).isin(patients_pk_broadcast_for_derived.value)),
                                        col("enc.enc_patient_id_derived")
                                    ).otherwise(None)
                                ) \
                                .drop("enc_id_join", "enc_patient_id_derived") # Noņemam pievienotās kolonnas pēc lietošanas

                            # Apvienojam labošanas loģiku vienā withColumn
                            corrected_df = temp_df_with_derived.withColumn(
                                fk_col,
                                when(
                                    col("temp_derived_patient_id_candidate").isNotNull(),
                                    col("temp_derived_patient_id_candidate") # Atvasinātā vērtība
                                ).when(
                                    lower(col(fk_col)).isin(ref_pk_broadcast.value), # Ja vērtība tagad ir atsaucē
                                    col(fk_col) # Atstājam esošo (derīgo) vērtību
                                ).otherwise(
                                    lit("UNKNOWN") # Paliek "UNKNOWN", ja nevar atvasināt vai nav atsaucē
                                )
                            ).drop("temp_derived_patient_id_candidate")

                            if initial_fk_issues_count > 0:
                                print(f"  Tabula '{name}', kolonnā '{fk_col}': {initial_fk_issues_count} atsauces integritātes problēmas labotas uz '{ref_table_name}.{pk_col}' (mēģināts atvasināt, citādi 'UNKNOWN').")
                                results_fix.append({
                                    "table_name": name,
                                    "quality_dimension": "Integ_Fix",
                                    "rule_name": f"{fk_col}_FK_Corrected_Chained",
                                    "issue_description": f"{initial_fk_issues_count} atsauces integritātes problēmas labotas ar tiešo validāciju un 'trīs tabulu variantu'.",
                                    "error_count": initial_fk_issues_count,
                                    "total_count": total_rows_before_correction,
                                    "error_percentage": round((initial_fk_issues_count / total_rows_before_correction) * 100, 2) if total_rows_before_correction > 0 else 0.0
                                })
                            else:
                                print(f"  Tabula '{name}', kolonnā '{fk_col}': Nav atrasti labojami atsauces integritātes jautājumi (trīs tabulu variants).")

                        else: # Citiem FK vai ja trīs tabulu variants nav piemērojams
                            corrected_df = corrected_df.withColumn(
                                fk_col,
                                when(
                                    lower(col(fk_col)).isin(ref_pk_broadcast.value), # Ja vērtība tagad ir atsaucē
                                    col(fk_col) # Atstājam esošo (derīgo) vērtību
                                ).otherwise(
                                    lit("UNKNOWN") # Ja nav atsaucē, tad "UNKNOWN"
                                )
                            )
                            if initial_fk_issues_count > 0:
                                print(f"  Tabula '{name}', kolonnā '{fk_col}': {initial_fk_issues_count} atsauces integritātes problēmas labotas uz '{ref_table_name}.{pk_col}' (tiešā labošana uz 'UNKNOWN').")
                                results_fix.append({
                                    "table_name": name,
                                    "quality_dimension": "Integ_Fix",
                                    "rule_name": f"{fk_col}_FK_Corrected_Direct",
                                    "issue_description": f"{initial_fk_issues_count} atsauces integritātes problēmas labotas (tiešā labošana uz 'UNKNOWN').",
                                    "error_count": initial_fk_issues_count,
                                    "total_count": total_rows_before_correction,
                                    "error_percentage": round((initial_fk_issues_count / total_rows_before_correction) * 100, 2) if total_rows_before_correction > 0 else 0.0
                                })
                            else:
                                print(f"  Tabula '{name}', kolonnā '{fk_col}': Nav atrasti labojami atsauces integritātes jautājumi (tiešā labošana).")

                    else:
                        print(f"  Brīdinājums: Nevar labot atsauces integritātes kolonnai '{fk_col}' tabulā '{name}' (trūkst atkarību).")
            else:
                print(f"  Nav definēti atsauces integritātes noteikumi tabulai '{name}'. Starptabulu labošana izlaista.")
            
            # Pēc FK labošanas, veicam checkpoint, lai "salauztu" izpildes grafu
            corrected_df = corrected_df.checkpoint()
            print(f"  Checkpoint veikts pēc FK labošanas tabulai '{name}'.")


            corrected_df_map[name] = corrected_df

    # 5. Laboto datu saglabāšana HDFS
    output_base_path = "/dati/synthea_kludainie_dati1_laboti"
    print(f"\n--- Laboto datu saglabāšana HDFS ceļā: {output_base_path} ---")

    for name, df in corrected_df_map.items():
        output_path = f"{output_base_path}/{name}"
        try:
            df.write.mode("overwrite").option("header", True).csv(output_path)
            print(f"  Tabula '{name}' veiksmīgi saglabāta kā CSV HDFS ceļā: {output_path}")
        except Exception as e:
            print(f"  Kļūda saglabājot tabulu '{name}' HDFS: {e}")

    # 6. Datu nolasīšana no HDFS (tikko labotie dati atskaites ģenerēšanai)
    df_map_for_report = {} # Vārdnīca datiem, ko izmantos atskaitei

    print(f"\nSāku nolasīt TIKKO LABOTOS datus no HDFS ceļā: {output_base_path} atskaites ģenerēšanai.")

    base_hdfs_path_obj_report = Path(output_base_path)

    if not fs.exists(base_hdfs_path_obj_report):
        print(f"Kļūda: HDFS bāses ceļš {output_base_path} neeksistē.")
    elif not fs.isDirectory(base_hdfs_path_obj_report):
        print(f"Kļūda: HDFS bāses ceļš {output_path} nav direktorijas.")
    else:
        for table_status in fs.listStatus(base_hdfs_path_obj_report):
            if table_status.isDirectory():
                table_dir = table_status.getPath().toString()
                table_name = os.path.basename(table_dir)
                print(f"  Atrasta tabulas direktorija: '{table_name}' ceļā {table_dir}")

                found_data_file = False
                csv_files_in_dir = [f.getPath().toString() for f in fs.listStatus(Path(table_dir)) if f.isFile() and f.getPath().getName().endswith(".csv")]

                if csv_files_in_dir:
                    try:
                        df = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(table_dir)
                        row_count = df.count()
                        if row_count > 0:
                            df_map_for_report[table_name] = df
                            print(f"    Veiksmīgi ielādēta tabula '{table_name}' ar {row_count} rindām.")
                            found_data_file = True
                        else:
                            print(f"    Brīdinājums: Ielādētā tabula '{table_name}' no {table_dir} ir tukša (0 rindas).")
                    except Exception as read_e:
                        print(f"    Kļūda lasot datus no {table_dir} (CSV): {read_e}")
                
                if not found_data_file:
                    print(f"  Brīdinājums: Tabulai '{table_name}' netika atrasts neviens atbalstīts datu fails vai neizdevās ielādēt datus.")

    # 7. Aprēķinām integritātes un pilnīguma procentus (Pēc Labošanas)
    integrity_report = []

    if not df_map_for_report:
        print("\nNav ielādētas tabulas atskaites ģenerēšanai, nevar ģenerēt atskaiti.")
    else:
        print("\n--- Datu Integritātes un Pilnīguma Atskaite (PĒC LABOŠANAS) ---")
        print("----------------------------------------------------------------")

        for name, df in df_map_for_report.items():
            print(f"\nApstrādājam tabulu: '{name}'")
            df.cache()
            total_rows = df.count()

            if total_rows == 0:
                print(f"  Tabula '{name}' ir tukša. Izlaižam integritātes un pilnīguma aprēķinus.")
                df.unpersist()
                continue

            # Pilnīguma aprēķins (Completeness)
            print("  Pilnīgums (tukšās/NULL/aizstājējvērtības):")
            for col_name in df.columns:
                null_or_empty_count = 0
                if df.schema[col_name].dataType == StringType():
                    null_or_empty_count = df.filter(is_effectively_empty_string(col(col_name))).count()
                else:
                    null_or_empty_count = df.filter(col(col_name).isNull()).count()
                
                completeness_percentage = ((total_rows - null_or_empty_count) / total_rows) * 100 if total_rows > 0 else 0
                integrity_report.append({
                    "table_name": name,
                    "metric_type": "Completeness",
                    "column_name": col_name,
                    "value": f"{completeness_percentage:.2f}% aizpildīts",
                    "raw_count": total_rows - null_or_empty_count,
                    "total_count": total_rows
                })
                print(f"    Kolonna '{col_name}': {completeness_percentage:.2f}% aizpildīts ({null_or_empty_count} tukšas/NULL/aizstājējvērtības)")

            # Atsauces integritātes aprēķins (Referential Integrity)
            print("  Atsauces integritāte:")
            if name in reference_rules:
                for fk_col, ref_table_name, pk_col in reference_rules[name]:
                    if ref_table_name in df_map_for_report:
                        ref_df = df_map_for_report[ref_table_name]
                        if fk_col in df.columns and pk_col in ref_df.columns and \
                           df.schema[fk_col].dataType == StringType() and ref_df.schema[pk_col].dataType == StringType():

                            # Uzskaitām svešās atslēgas vērtības, kas pēc labošanas JOPROJĀM ir "UNKNOWN"
                            # Tas norāda uz vērtībām, kuras nevarēja aizpildīt vai atrast atsaucē
                            unknown_fk_count = df.filter(lower(col(fk_col)) == "unknown").count()

                            # Filtrējam DatuFreimu, lai RI pārbaudi veiktu tikai ar potenciāli derīgām FK vērtībām
                            # (tās, kas nav "UNKNOWN" un nav tukšas/aizstājējas, jo tās jau bija "UNKNOWN")
                            df_for_ri_check = df.filter(
                                (~is_effectively_empty_string(col(fk_col))) & # Nav tukšas/UNKNOWN/N/A
                                (lower(col(fk_col)) != "unknown") # Nav "UNKNOWN"
                            )
                            total_fk_values_to_check = df_for_ri_check.count() # Potenciāli derīgās vērtības skaits

                            # Veicam RI pārbaudi (savienošanu) ar potenciāli derīgām FK vērtībām
                            successful_links_count = df_for_ri_check.alias("current_df").join(
                                ref_df.alias("ref_df"),
                                lower(col(f"current_df.{fk_col}")) == lower(col(f"ref_df.{pk_col}")),
                                "inner"
                            ).count()
                            
                            integrity_percentage = (successful_links_count / total_fk_values_to_check) * 100 if total_fk_values_to_check > 0 else 100
                            unlinked_valid_fk_count = total_fk_values_to_check - successful_links_count

                            # Ziņojam par atrastajām integritātes problēmām un to labojumiem
                            if unknown_fk_count > 0:
                                print(f"    Kolonnā '{fk_col}': {unknown_fk_count} vērtības ir 'UNKNOWN' (nevarēja labot).")
                                integrity_report.append({
                                    "table_name": name,
                                    "metric_type": "Referential Integrity (UNKNOWN FK - Post-Fix)",
                                    "column_name": f"{fk_col} (UNKNOWN Sentinel)",
                                    "value": f"{unknown_fk_count} vērtības ir 'UNKNOWN'",
                                    "raw_count": unknown_fk_count,
                                    "total_count": total_rows
                                })

                            if unlinked_valid_fk_count > 0:
                                print(f"    Atsauce '{fk_col}' uz '{ref_table_name}.{pk_col}': {integrity_percentage:.2f}% atbilstība ({unlinked_valid_fk_count} neatbilstošas *derīgas* vērtības).")
                                integrity_report.append({
                                    "table_name": name,
                                    "metric_type": "Referential Integrity (Unlinked Valid FK - Post-Fix)",
                                    "column_name": f"{fk_col} -> {ref_table_name}.{pk_col}",
                                    "value": f"{integrity_percentage:.2f}% atbilstība ({unlinked_valid_fk_count} neatbilstošas *derīgas* vērtības)",
                                    "raw_count": successful_links_count,
                                    "total_count": total_fk_values_to_check
                                })
                            else:
                                print(f"    Atsauce '{fk_col}' uz '{ref_table_name}.{pk_col}': 100.00% atbilstība (0 neatbilstošas *derīgas* vērtības).")
                                integrity_report.append({
                                    "table_name": name,
                                    "metric_type": "Referential Integrity (Unlinked Valid FK - Post-Fix)",
                                    "column_name": f"{fk_col} -> {ref_table_name}.{pk_col}",
                                    "value": f"100.00% atbilstība (0 neatbilstošas *derīgas* vērtības)",
                                    "raw_count": successful_links_count,
                                    "total_count": total_fk_values_to_check
                                })
                        else:
                            print(f"    Brīdinājums: Nevar pārbaudīt atsauces integritāti '{name}.{fk_col}' uz '{ref_table_name}.{pk_col}', jo trūkst vienas vai abas kolonnas vai tās nav StringType.")
                    else:
                        print(f"    Brīdinājums: Atsauces tabula '{ref_table_name}' nav ielādēta. Nevar pārbaudīt atsauces integritāti no '{name}.{fk_col}'.")
            else:
                print(f"    Tabulai '{name}' nav definēti atsauces integritātes noteikumi.")

            df.unpersist()
            print(f"  Tabulas '{name}' kešatmiņa atbrīvota.")

        print("\n--- Kopsavilkums (JSON formātā) ---")
        print(json.dumps(integrity_report, indent=2, default=str))
        print("------------------------------------")

finally:
    if spark:
        print("\nIzslēdzu Spark sesiju...")
        spark.stop()
        print("Spark sesija izslēgta.")