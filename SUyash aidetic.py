# Import required libraries
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, countDistinct
from elasticsearch import Elasticsearch

# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'clickstream_topic'

# Cassandra configuration
cassandra_contact_points = ['localhost']
cassandra_keyspace = 'clickstream_data'

# Spark configuration
spark_master = 'local[2]'  # Use local mode with 2 cores
spark_app_name = 'ClickstreamDataProcessor'

# Elasticsearch configuration
elasticsearch_host = 'localhost'
elasticsearch_port = 9200
elasticsearch_index = 'clickstream_index'

# Step 1: Ingestion - Kafka Consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    group_id='clickstream_group',
    auto_offset_reset='latest',
    enable_auto_commit=True
)

# Step 2: Data Storage - Cassandra
cluster = Cluster(cassandra_contact_points)
session = cluster.connect(cassandra_keyspace)

# Step 3: Periodic Processing - Spark
spark = SparkSession.builder \
    .master(spark_master) \
    .appName(spark_app_name) \
    .getOrCreate()

# Step 4: Indexing - Elasticsearch
es = Elasticsearch(hosts=[{'host': elasticsearch_host, 'port': elasticsearch_port}])

# Process clickstream data
for message in consumer:
    click_event = message.value.decode('utf-8')

    # Step 1: Ingestion - Store data in Cassandra
    session.execute(f"""
        INSERT INTO click_data (row_key, user_id, timestamp, url)
        VALUES (uuid(), '{click_event['user_id']}', '{click_event['timestamp']}', '{click_event['url']}');
    """)

# Step 3: Periodic Processing - Aggregate data with Spark
clickstream_data = spark.read\
    .format("org.apache.spark.sql.cassandra")\
    .options(table="click_data", keyspace=cassandra_keyspace)\
    .load()

# Perform aggregations
aggregated_data = clickstream_data.groupby('url', 'country')\
    .agg(countDistinct('user_id').alias('unique_users'), avg('time_spent').alias('avg_time_spent'))

# Step 4: Indexing - Index data in Elasticsearch
for row in aggregated_data.collect():
    document = {
        'url': row['url'],
        'country': row['country'],
        'unique_users': row['unique_users'],
        'avg_time_spent': row['avg_time_spent']
    }
    es.index(index=elasticsearch_index, body=document)
