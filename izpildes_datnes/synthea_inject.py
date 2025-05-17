import os
import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_sub, rand, when, concat, lit, date_format
    
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
    .appName("kludu_ieviesana_SyntheaHDFS") \
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

# Mapju norāde gan nolasīšanas, gan ierakstīšanas
INPUT_DIR = '/dati/synthea_csv'
OUTPUT_DIR = '/dati/synthea_kludainie_dati1'

# Kļūdu injekciju funkcijas#

# Apstrādā tabulu patients.csv, kurā injicē kļūdas atribūtos: BIRTHDATE, DEATHDATE un START/STOP
def inject_patients(df):
    # Dzimšanas datums pārveidots uz pagājušo gadsimtu, kur loģiski nevar būt neviens dzīvs vairs uz šodienu: kļūda 5% apmērā
    df = df.withColumn('BIRTHDATE',
        when(rand()<0.05, concat(lit('19'), date_format(col('BIRTHDATE'),'yy-MM-dd')))
        .otherwise(col('BIRTHDATE'))
    )
    # Nāves datums pirms dzimšanas datuma: kļūda 5% apmērā
    df = df.withColumn('DEATHDATE',
        when(rand()<0.05, date_sub(to_date(col('BIRTHDATE')),1).cast('string'))
        .otherwise(col('DEATHDATE'))
    )
    # Dzimums nav derīgs vai nav norādīts: kļūda 5% apmērā
    df = df.withColumn('GENDER',
        when(rand()<0.05, lit(None))
        .when(rand()<0.10, lit('X'))
        .otherwise(col('GENDER'))
    )
    # Rindu dublikātu izveide: kļūda 1% apmērā
    dup = df.sample(False,0.01,seed=1)
    return df.union(dup)

# Apstrādā tabulu encounters.csv, kurā injicē kļūdas atribūtos:
# ID bojāts - ID prefiksē ar 'bad-' tekstu, lai pārtrauktu unikalitāti
# Dublicētas rindas | Paraugi un 2% rindu apvienošanā
# Atribūts START injicēts uz invalid 5% un null uz 10%
# Atribūtam PATIENT injicēts uz invalid 5% un null uz 10%, lai zustu atsauces integritāte
def inject_encounters(df):
    df = df.withColumn('ID', when(rand()<0.05, concat(lit('bad-'),col('ID'))).otherwise(col('ID')))
    dup = df.sample(False,0.02,seed=2); df = df.union(dup)
    df = df.withColumn('START', when(rand()<0.05, lit('INVALID'))
                              .when(rand()<0.10, lit(None))
                              .otherwise(col('START')))
    df = df.withColumn('PATIENT', when(rand()<0.05, lit(None))
                                .when(rand()<0.10, lit('BAD'))
                                .otherwise(col('PATIENT')))
    return df

# Apstrādā tabulu conditions.csv, kurā injicē kļūdas atribūtos:
# Atribūtam PATIENT injicē null uz 5%, lai zustu atsauces integritāte
# Atribūtam ENCOUNTER injicē 0000 uz 5%
# Atribūtam STOP injicē dažāda formāta datumus, lai veidotos integritātes problēmas un nekonsikvence 5% formātā 'dd/MM/yyyy' un 10% 'yyyy.MM.dd'
# Atribūtam CODE injicē teksta virkni 'XXX123' uz 5%
def inject_conditions(df):
    df = df.withColumn('PATIENT', when(rand()<0.05, lit(None)).otherwise(col('PATIENT')))
    df = df.withColumn('ENCOUNTER', when(rand()<0.05, lit('0000')).otherwise(col('ENCOUNTER')))
    df = df.withColumn('STOP', when(rand()<0.05, date_format(col('STOP'),'dd/MM/yyyy'))
                              .when(rand()<0.10, date_format(col('STOP'),'yyyy.MM.dd'))
                              .otherwise(col('STOP')))
    df = df.withColumn('CODE', when(rand()<0.05, lit('XXX123')).otherwise(col('CODE')))
    return df

# Apstrādā tabulu medications.csv, kurā injicē kļūdas atribūtos:
# Atribūtam START injicē datuma formātu uz 'yyyy-MM-dd' vai NULL uz 10%
# Atribūtam STOP injicē datuma formātu uz 'yyyy-MM-dd' uz 5%
# Atribūtam ENCOUNTER injicē null uz 5%, lai zustu atsauces integritāte
# Atribūtam PATIENT injicē null uz 5%, lai zustu atsauces integritāte
def inject_medications(df):
    df = df.withColumn('START', when(rand()<0.05, date_format(col('START'),'yyyy-MM-dd'))
                                 .when(rand()<0.10, lit(None))
                                 .otherwise(col('START')))
    df = df.withColumn('STOP', when(rand()<0.05, date_format(col('STOP'),'yyyy-MM-dd'))
                                .otherwise(col('STOP')))
    df = df.withColumn('ENCOUNTER', when(rand()<0.05, lit(None)).otherwise(col('ENCOUNTER')))
    df = df.withColumn('PATIENT', when(rand()<0.05, lit(None)).otherwise(col('PATIENT')))
    return df

# Apstrādā tabulu allergies.csv, kurā injicē kļūdas atribūtos:
# Atribūtam STOP injicē viennu dienu atpakaļ, kā start diena ar funkcijas date_sub palīdzību uz 5%
# Atribūtam TYPE injicē UNKNOWN uz 5% vai null uz 10%
# Atribūtam ENCOUNTER injicē null uz 5%, lai zustu atsauces integritāte
# Atribūtam PATIENT injicē null uz 5%, lai zustu atsauces integritāte
def inject_allergies(df):
    df = df.withColumn('STOP', when(rand()<0.05, date_sub(col('START'),1)).otherwise(col('STOP')))
    df = df.withColumn('TYPE', when(rand()<0.05, lit('UNKNOWN'))
                                .when(rand()<0.10, lit(None))
                                .otherwise(col('TYPE')))
    df = df.withColumn('ENCOUNTER', when(rand()<0.05, lit(None)).otherwise(col('ENCOUNTER')))
    df = df.withColumn('PATIENT', when(rand()<0.05, lit(None)).otherwise(col('PATIENT')))
    return df

# Importē
from itertools import cycle

# Definē tabulu sarakstu ar to kļūdu injekcijas funkcijām, kas iepriekš tika nodefinētas
tables = [
    ('patients', inject_patients), # Apstrādā pacientu datus
    ('encounters', inject_encounters), # Apstrādā vizīšu datus
    ('conditions', inject_conditions), # Apstrādā diagnožu datus
    ('medications', inject_medications), # Apstrādā medikamentu datus
    ('allergies', inject_allergies) # Apstrādā alerģiju datus
]
# Pieejami datņu eksporta formāti (cikliski mainīsies katrai tabulai)
formats = ['avro','json','csv','parquet']
# Ciklisks iterators formātu maiņai
cycle_fmt = cycle(formats)

#Galvenā datu apstrādes cilpa
for name, fn in tables:
    fmt = next(cycle_fmt) # Izvēlas nākamo formātu ciklā
    df = spark.read.option('header',True).csv(f'{INPUT_DIR}/{name}.csv') # Ielādējam oriģinālos datus no CSV faila
    
    # Pielieto kļūdu injekcijas funkciju
    bad = fn(df) # bad satur datus ar ievietotām kļūdām
    out = f'{OUTPUT_DIR}/{name}_corrupted/{fmt}' # Sagatavo izvades ceļu
    # Saglabā datus atkarībā no formāta
    if fmt == 'csv': CSV gadījumā pievieno virsrakstu
        bad.write.option('header',True).mode('overwrite').csv(out)
    else:
        bad.write.format(fmt).mode('overwrite').save(out) # Pārējos formātos izmanto standarta saglabāšanu
    print(f'Pāraksta tabulu {name} kā {fmt} uz {out}') # Sniedz atsauces informāciju par izvades failiem

# Spark sesijas apstādināšana, lai SparkUI netiktu mākslīgi uzturēts RUNNING stāvoklis
spark.stop()