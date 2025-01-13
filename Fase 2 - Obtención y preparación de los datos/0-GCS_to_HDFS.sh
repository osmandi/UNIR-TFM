# Load environment variables
. ./.env

# Create folders in the HDFS to raw data
hdfs dfs -mkdir -p /TFM/data/raw
hdfs dfs -mkdir -p /TFM/data/bronze
hdfs dfs -mkdir -p /TFM/data/silver

# Folders to save output files
hdfs dfs -mkdir -p /TFM/output

# Copy file from GCS to local
mkdir -p /tmp/TFM
gcloud storage cp $GCS_BUCKET/raw/data.zip /tmp/TFM

# Unzip file
unzip /tmp/TFM/data.zip -d /tmp/TFM/

# Copy CSV files from local to HDFS
hdfs dfs -copyFromLocal /tmp/TFM/data/*.csv /TFM/data/raw

# Remove file of tmp folder
rm -rf /tmp/TFM