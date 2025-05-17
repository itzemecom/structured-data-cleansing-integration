import os

local_dir = "/opt/workspace/synthea_izejas_dati/CSV"
hdfs_dir = "/dati/synthea_csv"

# Izveido HDFS mapi
!hdfs dfs -mkdir -p {hdfs_dir}

# Pārsūta visus CSV failus uz HDFS
for file in os.listdir(local_dir):
    if file.endswith(".csv"):
        local_file = os.path.join(local_dir, file)
        hdfs_file = f"{hdfs_dir}/{file}"
        !hdfs dfs -put -f {local_file} {hdfs_file}