from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType
import nltk, json
from pyspark.sql.functions import udf, explode, col, to_json, struct

import os

# downloading NLTK data
nltk.download("punkt")
nltk.download("averaged_perceptron_tagger")
nltk.download("maxent_ne_chunker")
nltk.download("words")


def extract_entities_nltk(text):
    if not text or not isinstance(text, str):
        return []

    try:
        tokens = nltk.word_tokenize(text)
        pos_tags = nltk.pos_tag(tokens)
        chunks = nltk.ne_chunk(pos_tags)

        entities = []
        for chunk in chunks:
            if hasattr(chunk, 'label'):
                entity = ' '.join(c[0] for c in chunk)
                entities.append(entity)
        return entities
    except Exception as e:
        print(f"Error processing text: {str(e)}")
        return []


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 4:
        print("Usage: structured_streaming.py <bootstrap-servers> <subscribe-type> <topics>")
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    # initializing Spark Session
    spark = SparkSession \
        .builder \
        .appName("StructuredKafkaNER") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # registring UDF
    extract_entities_udf = udf(extract_entities_nltk, ArrayType(StringType()))

    # reading from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option(subscribeType, topics) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # processing streams
    parsed = df.selectExpr("CAST(value AS STRING) as text")

    # extracting entities
    entities = parsed \
        .withColumn("entities", extract_entities_udf(col("text"))) \
        .withColumn("entity", explode(col("entities"))) \
        .groupBy("entity") \
        .count() \
        .orderBy("count", ascending=False)

    # converting to JSON format
    json_output = entities \
        .select(
        to_json(struct(
            col("entity").alias("key"),
            col("count").alias("value")
        )).alias("value")
    )

    # writing to Kafka
    query = json_output \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrapServers) \
        .option("topic", "topic2") \
        .option("checkpointLocation", "/tmp/checkpoints") \
        .outputMode("complete") \
        .start()

    query.awaitTermination()