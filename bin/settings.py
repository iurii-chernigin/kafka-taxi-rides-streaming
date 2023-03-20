import pyspark.sql.types as T


OUTPUT_TOPIC_UNION_RIDES = ""
OUTPUT_TOPIC_TOP_PICKUP_LOCATION = ""
INPUT_TOPIC_GREEN_RIDES = "rides_green"
INPUT_TOPIC_FHV_RIDES = "rids_fhv"

KAFKA_BOOTSTRAP_SERVER = "<dns>:9092"
KAFKA_CLUSTER_KEY = ""
KAFKA_CLUSTER_SECRET = ""


SCHEMA_REGISTRY_ADDRESS = "https://<dns>"
SCHEMA_REGISTRY_KEY = ""
SCHEMA_REGISTRY_SECRET = "" 
SCHEMA_REGISTRY_OPTIONS = {
  "confluent.schema.registry.basic.auth.credentials.source": 'USER_INFO',
  "confluent.schema.registry.basic.auth.user.info": "{}:{}".format(SCHEMA_REGISTRY_KEY, SCHEMA_REGISTRY_SECRET)
}


SCHEMA_GREEN_RIDE = T.StructType([
        T.StructField("pickup_datetime", T.StringType()),
        T.StructField("dropoff_datetime", T.StringType()),
        T.StructField("vendor_id", T.StringType()),
        T.StructField("pickup_location_id", T.IntegerType()),
        T.StructField("dropoff_location_id", T.IntegerType()),
        T.StructField("sended_to_kafka_ts", T.IntegerType())
    ])

SCHEMA_FHV_RIDE = T.StructType([
        T.StructField("pickup_datetime", T.TimestampType()),
        T.StructField("dropoff_datetime", T.TimestampType()),
        T.StructField("pickup_location_id", T.IntegerType()),
        T.StructField("dropoff_location_id", T.IntegerType()),
        T.StructField("sended_to_kafka_ts", T.IntegerType())
    ])

