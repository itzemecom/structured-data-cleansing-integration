# structured-data-cleansing-integration
Datu integrācijas un attīrīšanas metodes strukturētiem lielapjoma datiem, RTU, 2025
# Hadoop & Spark Klasteris ar JupyterLab Datu Apstrādei

Šis projekts nodrošina Docker Compose konfigurāciju, lai izveidotu lokālu Hadoop (HDFS) un Spark klasteri, kā arī JupyterLab vidi datu analīzei un apstrādei. Papildus tam, šīs instrukcijas parāda, kā no JupyterLab pieslēgties ārējai PostgreSQL datubāzei, lai izveidotu specifisku tabulu datu kvalitātes vēstures glabāšanai. Tas ir paredzēts, lai nodrošinātu reproducējamu un viegli pārvaldāmu vidi datu zinātnes eksperimentiem.

## 1. Komponentes

Projekts sastāv no šādiem Docker servisiem:

**Hadoop NameNode (`namenode`):**
Attēls: `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`.
Tas ir galvenais mezgls HDFS failu sistēmai, kas pārvalda failu sistēmas metadatus.
UI pieejams: `http://localhost:9870`.
RPC ports: `9000`.

**Hadoop DataNode (`datanode`):**
Attēls: `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8`.
Šis serviss uzglabā faktiskos datus HDFS blokos.

**Spark Master (`spark-master`):**
Attēls: `bitnami/spark:3.4.2`.
Koordinē Spark aplikāciju izpildi klasterī.
UI pieejams: `http://localhost:8080`.
RPC ports: `7077`.

**Spark Worker (3 instances: `spark-worker-1`, `spark-worker-2`, `spark-worker-3`):**
Attēls: `bitnami/spark:3.4.2`.
Šie servisi izpilda Spark aplikāciju uzdevumus (tasks). Katram workerim ir savs UI:
`spark-worker-1` UI: `http://localhost:8081`.
`spark-worker-2` UI: `http://localhost:8082`.
`spark-worker-3` UI: `http://localhost:8083`.

**JupyterLab (`jupyterlab`):**
Būvēts no lokālā `jupyter_magistrs2025.Dockerfile`.
Nodrošina interaktīvu Python vidi ar PySpark, Pandas, scikit-learn, `psycopg2-binary` (PostgreSQL savienojumam) un citām datu zinātnes bibliotēkām.
Pieslēdzas Spark Master un HDFS.
UI pieejams: `http://localhost:8888` (bez paroles).

**Piezīme par PostgreSQL:** PostgreSQL datubāze **netiek** pārvaldīta ar šo `docker-compose.yml` konfigurāciju. Tiek pieņemts, ka jums ir pieejama PostgreSQL instance (piemēram, lokāli instalēta uz jūsu hosta mašīnas vai citā serverī), kurai JupyterLab varēs pieslēgties.

## 2. Priekšnosacījumi

Lai veiksmīgi palaistu šo projektu, ir nepieciešams:  
Instalēt [Docker](https://docs.docker.com/get-docker/).   
Pieejama PostgreSQL datubāzes instance un tās pieslēguma dati (hosts, ports, lietotājs, parole, datubāzes nosaukums).  
Instalēts Git (lai klonētu repozitoriju, ja tas ir nepieciešams).  

## 3. Projekta struktūra

Pirms palaišanas, pārliecinieties, ka Jūsu projekta direktorijā ir šāda failu un mapju struktūra:

├── docker-compose.yml          # Docker konfigurācija  
├── jupyter_magistrs2025.Dockerfile  # JupyterLab Dockerfile  
├── core-site.xml              # Hadoop konfigurācija  
├── .env                       # Vides mainīgie  
├── synthea_izejas_dati/       # Izejas dati  
├── synthea_kludainie_dati/    # Kļūdainie dati  
└── izpildes_datnes/    # Izpildāmās datnes hetegorēnu datu attīrīšanā un integrācijā  

## 4. Konfigurācija

Šajā sadaļā aprakstīti galvenie konfigurācijas datnes.

### `core-site.xml`

Šī datne ir nepieciešama, lai Spark servisi (Master, Workers, JupyterLab) varētu sazināties ar HDFS. Izveidojiet datni `core-site.xml` projekta saknes direktorijā ar šādu saturu:
```
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
</configuration>
```
## `.env` datne
Šī datne tiek izmantota, lai definētu vides mainīgos. Tas ir īpaši noderīgi sensitīvu datu, piemēram, PostgreSQL paroļu glabāšanai.
Izveidojiet .env datni projekta saknes direktorijā. Piemērs saturam:
```
# Hadoop/Spark (ja nepieciešams kādi papildu globāli mainīgie)
# COMPOSE_PROJECT_NAME=mans_hadoop_projekts

# PostgreSQL pieslēguma dati (pielāgojiet savai instancei)
MY_POSTGRES_HOST=host.docker.internal # Ja PGSQL darbojas uz Docker hosta mašīnas
# MY_POSTGRES_HOST=jusu_db_servera_ip_vai_nosaukums # Ja PGSQL ir citur
MY_POSTGRES_PORT=5432
MY_POSTGRES_USER=jusu_postgres_lietotajs
MY_POSTGRES_PASSWORD=jusu_slepena_un_drosa_parole
MY_POSTGRES_DB=jusu_datubazes_nosaukums
```
**Svarīgi:** Aizstājiet piemēra vērtības ar saviem faktiskajiem PostgreSQL pieslēguma datiem. Šis fails tiek ielādēts katram servisam, kas norādīts ar `env_file: - .env docker-compose.yml` datnē.
## Datu mapes
`synthea_izejas_dati/:` Šī mape tiks piesaistīta `/opt/workspace/synthea_izejas_dati` JupyterLab konteinerī. Šeit varat ievietot datus, ko vēlaties apstrādāt.  
`synthea_kludainie_dati/:` Šī mape tiks piesaistīta `/opt/workspace/synthea_kludainie_dati` JupyterLab konteinerī. 
`jupyter_magistrs2025.Dockerfile` Šis `Dockerfile` definē pielāgotu JupyterLab vidi.  
Galvenās tā funkcijas ir:  
Bāzējas uz `ubuntu:22.04`.  
Instalē Java 11, Python 3 un nepieciešamās sistēmas bibliotēkas.  
Izveido Python virtuālo vidi un instalē plašu datu zinātnes bibliotēku klāstu, tostarp `pyspark==3.4.2, delta-spark==2.4.0, jupyterlab, psycopg2-binary (PostgreSQL savienojumam), pandas, numpy, scikit-learn, matplotlib, seaborn, pyarrow, fastparquet, polars, great-expectations, ydata-profiling` un citas.  
Lejupielādē un konfigurē Apache Spark `${SPARK_VERSION}` (3.4.2) un Hadoop CLI `${HADOOP_CLI_VERSION}` (3.3.6).  
Kopē `core-site.xml` no būvēšanas konteksta uz Spark konfigurācijas direktoriju konteinerī.  
Noklusējuma darba direktorija konteinerī ir `/opt/workspace`.  
Startē JupyterLab uz porta 8888 bez autentifikācijas.  
## 5. Procesa palaišana
Lai palaistu projektu, izpildiet šādas darbības:

**Failu sagatavošana:**
Pārliecinieties, ka visi augstāk minētie faili (`docker-compose.yml`, `jupyter_magistrs2025.Dockerfile`, `core-site.xml`, `.env`) un mapes (`synthea_izejas_dati`, `synthea_kludainie_dati`) atrodas Jūsu projekta saknes direktorijā.

**Būvēšana un servisu palaišana:**
Atveriet termināli projekta saknes direktorijā un izpildiet komandu:
```docker-compose up --build -d```

Komanda `--build` nodrošinās, ka `jupyterlab` attēls tiek būvēts no `jupyter_magistrs2025.Dockerfile` (vai atjaunots, ja Dockerfile ir mainīts).
Opcija -d palaidīs konteinerus fona režīmā.  

**Statusa pārbaude:**
Lai redzētu aktīvos konteinerus, izmantojiet komandu:
```docker-compose ps```

Lai skatītu kāda konkrēta servisa žurnālus (piemēram, `jupyterlab`), izmantojiet komandu:
```docker-compose logs -f jupyterlab```

## 6. Piekļuve servisiem
Pēc veiksmīgas palaišanas, servisiem var piekļūt šādās adresēs:  
Hadoop NameNode UI: http://localhost:9870  
Spark Master UI: http://localhost:8080  
Spark Worker 1 UI: http://localhost:8081  
Spark Worker 2 UI: http://localhost:8082  
Spark Worker 3 UI: http://localhost:8083  
JupyterLab: http://localhost:8888 (***Parole nav nepieciešama***)  

## 7. Datu pārvaldība un darbvieta
**Koplietotā darbvieta (`shared-workspace`):**
Visi servisi (Hadoop NameNode/DataNode, Spark Master/Workers, JupyterLab) piesaista Docker sējumu `shared-workspace` (ar nosaukumu `lokala_datu_kratuve` uz Jūsu datora) pie `/opt/workspace` konteineros.
Tas nozīmē, ka faili, kas saglabāti `/opt/workspace` vienā konteinerī (piemēram, JupyterLab), būs pieejami arī citos servisos šajā ceļā.
Dati šajā sējumā saglabāsies arī pēc konteineru apturēšanas un noņemšanas, ja vien sējums netiek dzēsts manuāli (`docker volume rm lokala_datu_kratuve`).

**Lokālās mapes JupyterLab:**
Jūsu lokālā mape synthea_izejas_dati ir piesaistīta pie /opt/workspace/synthea_izejas_dati JupyterLab konteinerī.
Jūsu lokālā mape synthea_kludainie_dati ir piesaistīta pie /opt/workspace/synthea_kludainie_dati JupyterLab konteinerī.
Darbs ar HDFS:
No JupyterLab (izmantojot PySpark), Jūs varat lasīt un rakstīt datus uz HDFS, izmantojot ceļus, kas sākas ar hdfs://namenode:9000/. Piemēram, hdfs://namenode:9000/user/jovyan/data.csv.
Lai manuāli pārvaldītu failus HDFS, varat pieslēgties namenode konteinerim:

```docker exec -it namenode bash```

Un tad izmantot HDFS komandas, piemēram, `hdfs dfs -ls /` vai `hdfs dfs -put /opt/workspace/mans_fails.txt /user/.`

**Darbs ar Ārējo PostgreSQL Datubāzi:**
Šī konfigurācija neietver PostgreSQL serveri. Jums ir nepieciešama jau esoša PostgreSQL instance.
No JupyterLab Jūs varat pieslēgties šai ārējai datubāzei, izmantojot ``Python`` valodas `psycopg2` bibliotēku.
Pieslēguma dati (hosts, ports, lietotājs, parole, datubāzes nosaukums) jānorāda `.env` failā un tie tiek izmantoti Python skriptā JupyterLab vidē.
## 8. PostgreSQL tabulas izveide
No JupyterLab Jūs varat pieslēgties savai ārējai PostgreSQL datubāzei un izveidot nepieciešamo tabulu datu kvalitātes vēstures glabāšanai vai tā automātiski tiek veidota, ja palaidīsiet attiecīgos skriptus, kas nodrošina datu novērtēšanu un attīrīšanu. Dati šajā tabulā tiks ievietoti no cita, atsevišķa skripta.
### Python Skripts Tabulas Izveidei, ja gadījumā vēlaties manuāli veidot
Izveidojiet jaunu Jupyter notebook (`.ipynb`) JupyterLab vidē (`http://localhost:8888`) un izpildiet šādu Python kodu. **Pārliecinieties, ka Jūsu .env fails ir pareizi konfigurēts ar Jūsu PostgreSQL pieslēguma datiem.**
```import psycopg2
import os
# Ja .env fails atrodas projekta saknes direktorijā (/opt/workspace JupyterLab konteinerī)
# un vēlaties to ielādēt tieši notebook'ā (nav obligāti, ja docker-compose nodod mainīgos):
# from dotenv import load_dotenv
# load_dotenv(dotenv_path='/opt/workspace/.env') # Pielāgojiet ceļu, ja nepieciešams

# --- PostgreSQL Pieslēguma Konfigurācija ---
db_host = os.getenv('MY_POSTGRES_HOST', 'host.docker.internal')
db_port = os.getenv('MY_POSTGRES_PORT', '5432')
db_name = os.getenv('MY_POSTGRES_DB') # Jābūt definētam .env vai šeit
db_user = os.getenv('MY_POSTGRES_USER') # Jābūt definētam .env vai šeit
db_password = os.getenv('MY_POSTGRES_PASSWORD') # Jābūt definētam .env vai šeit

# Pārbaude, vai nepieciešamie mainīgie ir iestatīti
if not all([db_name, db_user, db_password]):
    missing_vars_list = [var_name for var_name, var_value in [
        ('DB_NAME', db_name), ('DB_USER', db_user), ('DB_PASSWORD', db_password)
    ] if not var_value]
    raise ValueError(f"Trūkst PostgreSQL pieslēguma mainīgo: {', '.join(missing_vars_list)}. Pārbaudiet .env failu un docker-compose.yml vides mainīgos.")

conn = None
try:
    print(f"Mēģina pieslēgties PostgreSQL: host={db_host}, port={db_port}, dbname='{db_name}', user='{db_user}'")
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    print(f"Veiksmīgi pieslēgts PostgreSQL datubāzei '{db_name}'!")

    cur = conn.cursor()

    # --- Tabulas 'synthea_data_quality_2025' Izveide ---
    # Dati šajā tabulā tiks ievietoti no cita skripta.
    table_name = "datu_kvalitates_vesture"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
    """
    cur.execute(create_table_query)
    conn.commit() # Apstiprinām tabulas izveidi
    print(f"Tabula '{table_name}' pārbaudīta/izveidota veiksmīgi.")

    # Piemērs, kā pārbaudīt, vai tabula eksistē (pēc izveides)
    cur.execute(f"SELECT to_regclass('{table_name}');")
    table_exists = cur.fetchone()[0]
    if table_exists:
        print(f"Pārbaude: Tabula '{table_name}' eksistē datubāzē.")
    else:
        print(f"Pārbaude: Kļūda! Tabula '{table_name}' netika atrasta pēc izveides mēģinājuma.")

    cur.close()

except psycopg2.OperationalError as e:
    print(f"KĻŪDA: Nevar pieslēgties PostgreSQL datubāzei.")
    print(f"Pārbaudiet pieslēguma parametrus un vai datubāzes serveris ir pieejams no šī konteinera.")
    print(f"Host: {db_host}, Port: {db_port}, Datubāze: '{db_name}', Lietotājs: '{db_user}'")
    print(f"Sistēmas kļūdas ziņojums: {e}")
except (Exception, psycopg2.Error) as error:
    print(f"Kļūda strādājot ar PostgreSQL: {error}")
    if conn:
        conn.rollback() # Atceļam transakciju, ja kaut kas nogāja greizi
finally:
    if conn:
        conn.close()
        print("\nPostgreSQL savienojums aizvērts.")
```
**Piezīmes par Python skriptu:**
Skripts tikai izveido tabulu `synthea_data_quality_2025`, tā patām nepieciešams citas tabulas:`error_synthea_data_quality_2025`. Datu ievietošanas (`INSERT`) daļa ir noņemta, jo dati tiks ievietoti no cita skripta.
`.env` failam jābūt pareizi konfigurētam ar Jūsu PostgreSQL pieslēguma informāciju.
Skripts pieņem, ka datubāze, kas norādīta `db_name` vides mainīgajā, jau eksistē Jūsu PostgreSQL serverī.
## 9. Apturēšana
Lai apturētu un noņemtu Docker Compose definētos konteinerus:
```docker-compose down```

Ja vēlaties noņemt arī Docker sējumus ( uzmanību: tas dzēsīs HDFS datus un shared-workspace saturu, kas nav Jūsu lokālajās piesaistītajās mapēs! PostgreSQL dati netiks ietekmēti, jo tie ir ārēji.):
```docker-compose down -v```

## 10. Reproducējamība
Šī konfigurācija nodrošina augstu Hadoop/Spark vides reproducējamības līmeni:
**Docker Attēli:** Fiksētas programmatūras versijas tiek izmantotas.
**Docker Compose:** Infrastruktūra (servisi, tīkli, sējumi) tiek definēta konsekventi.
**Konfigurācijas Faili:** `core-site.xml` nodrošina HDFS pieslēgumu.
PostgreSQL vides reproducējamība un pārvaldība ir atkarīga no tā, kā s pārvaldāt savu ārējo PostgreSQL instanci.
## 11. Problēmu Risināšana
**Portu konflikti:**
Ja kāds no norādītajiem portiem (9870, 9000, 8080-8083, 8888) jau tiek izmantots su sistēmā, `docker-compose up` neizdosies. Jums būs jāatbrīvo attiecīgais ports vai jāmaina porta kartēšana `docker-compose.yml` failā.
**Resursu ierobežojumi:**
Hadoop un Spark var patērēt daudz RAM. Pārliecinieties, ka su Docker Desktop (vai Docker dzinējam Linux) ir piešķirts pietiekami daudz resursu.
**Failu tiesības:**
Ja rodas problēmas ar failu rakstīšanu `shared-workspace` vai piesaistītajās mapēs no `JupyterLab`, tas varētu būt saistīts ar failu īpašumtiesībām/atļaujām. jupyterlab konteineris tiek palaists ar root lietotāju, kas parasti mazina šādas problēmas, bet ir vērts paturēt prātā.
**PostgreSQL savienojuma kļūdas:**
Pārbaudiet `db_host`, `db_port`, `db_name`, `db_user`, `db_password` Python skriptā un/vai `.env` failā.
Pārliecinieties, ka Jūsu PostgreSQL serveris ir pieejams no JupyterLab konteinera (tīkla konfigurācija, ugunsmūris). host.docker.internal var nedarboties visās Linux konfigurācijās; šādā gadījumā var būt nepieciešams izmantot Docker hosta IP adresi.
Pārbaudiet PostgreSQL servera žurnālus, lai redzētu, vai ir kādi savienojuma mēģinājumi vai kļūdas.
Pārliecinieties, ka norādītā datubāze (db_name) eksistē un lietotājam db_user ir tiesības tai pieslēgties un veikt CREATE TABLE operācijas.
## 12. Papildu Ieteikumi
**`.gitignore:`**
Ja šis projekts būs Jūsu Git repozitorijā, apsveriet iespēju pievienot .env un, iespējams, lokala_datu_kratuve (ja tā ir mape, nevis tikai Docker sējuma nosaukums) .gitignore failam, lai nejauši nepublicētu sensitīvus datus vai lielus datu apjomus.
**JupyterLab lietotājs:**
`jupyterlab` konteinerī tiek izmantots `root` lietotājs. Tas vienkāršo failu tiesību jautājumus, bet ražošanas vidēs parasti tiek izmantots neprivileģēts lietotājs. Jūsu mācību/eksperimentu videi tas ir pieņemami.
**Detalizētāka `.env` izmantošana:**
Ja nākotnē radīsies vajadzība konfigurēt, piemēram, Spark workeru atmiņu vai kodolu (Core) skaitu caur .env, Jūs varētu `docker-compose.yml` failā nomainīt fiksētās vērtības (piem., `SPARK_WORKER_MEMORY=1G`) uz mainīgajiem (piem., `SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY_CONFIG:-1G}`) un definēt `SPARK_WORKER_MEMORY_CONFIG` `.env` failā.
