# spark-submit with a Spark distribution supporting Structured Streaming.
# Place JSON files in input/events; new files are picked up as they arrive.
from pyspark.sql import SparkSession, functions as F
spark=SparkSession.builder.appName('portfolio-events').getOrCreate()
schema='event_id STRING, device_id STRING, event_time STRING, temperature DOUBLE'
source=(spark.readStream.schema(schema).json('input/events')
    .withColumn('timestamp',F.to_timestamp('event_time'))
    .filter('event_id IS NOT NULL AND device_id IS NOT NULL AND timestamp IS NOT NULL')
    .withWatermark('timestamp','10 minutes').dropDuplicates(['event_id']))
summary=(source.groupBy(F.window('timestamp','5 minutes'),'device_id')
    .agg(F.avg('temperature').alias('avg_temperature'),F.count('*').alias('events')))
query=(summary.writeStream.outputMode('append').format('parquet')
    .option('path','output/aggregates').option('checkpointLocation','.checkpoints/aggregates').start())
query.awaitTermination()
