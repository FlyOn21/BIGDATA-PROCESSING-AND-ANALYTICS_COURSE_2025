from __future__ import annotations

import logging
import os
import tempfile
import contextlib

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import coalesce, from_unixtime, expr
from pyspark.sql.functions import col, from_json, to_timestamp, when, window, count, desc
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)





class SparkJsonSchemaStreamProcessor:
    """
    Standalone class that:
      - Reads from two Kafka topics (transactions, user_activity)
      - Parses JSON payloads using explicit StructType schemas
      - Handles both RFC3339 timestamps and Unix timestamps in milliseconds
      - Applies a 10-minute watermark on the event-time column 'timestamp'
    """

    def __init__(
            self,
            bootstrap_servers: str = "127.0.0.1:9092",
            app_name: str = "KafkaJSONStructuredStreaming",
            master: str | None = "local[*]",
            session_tz: str = "Europe/Kyiv",
            starting_offsets: str = "earliest",
    ) -> None:

        problematic_vars = ['PYSPARK_SUBMIT_ARGS', 'SPARK_SUBMIT_OPTS']
        for var in problematic_vars:
            if var in os.environ:
                logger.info(f"Clearing {var}: {os.environ[var]}")
                del os.environ[var]

        if 'JAVA_HOME' not in os.environ:
            java_home = '/usr/lib/jvm/java-21-openjdk-amd64'
            if os.path.exists(java_home):
                os.environ['JAVA_HOME'] = java_home
                logger.info(f"Set JAVA_HOME to: {java_home}")

        temp_dir = tempfile.mkdtemp(prefix="spark_")
        os.environ['SPARK_LOCAL_DIRS'] = temp_dir

        builder = (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", session_tz)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.driver.maxResultSize", "1g")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.jars.ivy", f"{temp_dir}/ivy")
            .config("spark.sql.streaming.checkpointLocation", f"{temp_dir}/checkpoints")
        )

        if master:
            builder = builder.master(master)

        logger.info("Initializing Spark session...")

        try:
            self.spark: SparkSession = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")
            logger.info(f"Spark session created successfully. Version: {self.spark.version}")

            try:
                self.spark.readStream.format("kafka")
                logger.info("Kafka connector is available!")
            except Exception as kafka_error:
                logger.info(f"WARNING: Kafka connector not available: {kafka_error}")
                logger.info(
                    "Make sure to run with: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2")

        except Exception as e:
            logger.info(f"Failed to create Spark session: {e}")
            raise e

        self.bootstrap_servers = bootstrap_servers
        self.starting_offsets = starting_offsets

    @staticmethod
    def transactions_schema() -> StructType:
        """Schema for transaction messages - handles both string and numeric timestamps"""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("merchant", StringType(), False),
            StructField("currency", StringType(), False),
            StructField("timestamp", StringType(), True),
            StructField("is_fraud", BooleanType(), False),
        ])

    @staticmethod
    def user_activity_schema() -> StructType:
        """Schema for user activity messages"""
        return StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("device", StringType(), False),
            StructField("browser", StringType(), False),
            StructField("timestamp", StringType(), True),
        ])

    @staticmethod
    def _parse_timestamp(col_expr):
        """
        Parse timestamp that can be either:
        1. RFC3339 string (from JSON schema)
        2. Unix timestamp in milliseconds (from actual data generator)

        Returns: TimestampType column
        """
        # Try different timestamp formats, ensuring we always return TimestampType
        return coalesce(
            to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ssXXX"),
            to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            to_timestamp(col_expr, "yyyy-MM-dd'T'HH:mm:ssX"),
            when(col_expr.rlike("^[0-9]+$"),
                 when(col_expr.cast("bigint") > 1000000000000,
                      from_unixtime(col_expr.cast("bigint") / 1000).cast("timestamp"))
                 .otherwise(from_unixtime(col_expr.cast("bigint")).cast("timestamp"))
                 )
        )

    def read_kafka_streams(
            self,
            transactions_topic: str = "transactions",
            user_activity_topic: str = "user_activity",
    ) -> dict[str, DataFrame]:
        """
        Task 1: Reading from Kafka
        Reading from two Kafka topics using Spark Structured Streaming
        """
        logger.info(f"Connecting to Kafka cluster at {self.bootstrap_servers}")
        logger.info(f"Reading from topics: {transactions_topic}, {user_activity_topic}")

        transactions = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", transactions_topic)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS json_str", "timestamp as kafka_timestamp")
        )

        user_activity = (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", user_activity_topic)
            .option("startingOffsets", self.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
            .selectExpr("CAST(value AS STRING) AS json_str", "timestamp as kafka_timestamp")
        )

        return {"transactions_raw": transactions, "user_activity_raw": user_activity}

    def parse_transactions_json_with_watermark(
            self,
            transactions_json_df: DataFrame,
            watermark: str = "10 minutes",
    ) -> DataFrame:
        """
        Task 2: Transaction Processing
        Transform data from Kafka to DataFrame with required columns
        and set proper timestamp type
        """
        logger.info("Parsing transactions JSON and applying watermark...")
        parsed = (
            transactions_json_df
            .select(from_json(col("json_str"), self.transactions_schema()).alias("data"), "kafka_timestamp")
            .select("data.*", "kafka_timestamp")
        )

        parsed = parsed.withColumn("timestamp", self._parse_timestamp(col("timestamp")))

        parsed = parsed.select(
            "transaction_id",
            "user_id",
            "amount",
            "merchant",
            "timestamp",
            "is_fraud"
        )

        return parsed.withWatermark("timestamp", watermark)

    def parse_user_activity_json_with_watermark(
            self,
            user_activity_json_df: DataFrame,
            watermark: str = "10 minutes",
    ) -> DataFrame:
        """Parse user activity JSON with watermark"""
        logger.info("Parsing user activity JSON and applying watermark...")

        parsed = (
            user_activity_json_df
            .select(from_json(col("json_str"), self.user_activity_schema()).alias("data"), "kafka_timestamp")
            .select("data.*", "kafka_timestamp")
        )

        parsed = parsed.withColumn("timestamp", self._parse_timestamp(col("timestamp")))

        return parsed.withWatermark("timestamp", watermark)

    @staticmethod
    def analyze_user_activity_with_windows(
            user_activity_parsed: DataFrame,
            window_duration: str = "10 minutes",
            slide_duration: str = "5 minutes"
    ) -> DataFrame:
        """
        Task 3: User Activity Analysis
        Analyze user activity with sliding window aggregation:
        - Group by event_type (click, add_to_cart, purchase)
        - Count events per user_id in time windows
        - Use sliding window aggregation
        """
        logger.info(f"Analyzing user activity with {window_duration} windows, sliding every {slide_duration}...")

        return (
            user_activity_parsed.groupBy(
                window(col("timestamp"), window_duration, slide_duration),
                col("user_id"),
                col("event_type"),
            )
            .agg(count("*").alias("event_count"))
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_id"),
                col("event_type"),
                col("event_count"),
            )
            .orderBy(desc("window_start"), desc("event_count"))
        )

    @staticmethod
    def analyze_event_type_summary(
            user_activity_parsed: DataFrame,
            window_duration: str = "10 minutes"
    ) -> DataFrame:
        """
        Create summary of event types in time windows
        """
        logger.info(f"Creating event type summary with {window_duration} windows...")

        return (
            user_activity_parsed.groupBy(
                window(col("timestamp"), window_duration), col("event_type")
            )
            .agg(
                count("*").alias("total_events"),
                count(col("user_id")).alias("unique_users"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("event_type"),
                col("total_events"),
                col("unique_users"),
            )
            .orderBy(desc("window_start"), desc("total_events"))
        )
    
    @staticmethod
    def detect_fraud_transactions(
            transactions_parsed: DataFrame
    ) -> DataFrame:
        """
        Task 4: Fraud Detection
        Apply business logic to detect fraudulent transactions:
        - Transactions over $1000
        - Merchant = "Amazon"
        - is_fraud == true
        """
        logger.info("Applying fraud detection business logic...")

        return transactions_parsed.filter(
            (col("amount") > 1000)  # Transactions over $1000
            & (col("merchant") == "Amazon")  # Amazon transactions
            & (col("is_fraud") == True)  # Already marked as fraud
        ).withColumn(
            "fraud_reason",
            when(col("amount") > 1000, "High Amount")
            .when(col("merchant") == "Amazon", "Amazon Transaction")
            .when(col("is_fraud") == True, "Flagged as Fraud")
            .otherwise("Multiple Reasons"),
        )

    @staticmethod
    def save_fraud_results_locally(
            fraud_transactions: DataFrame,
            output_path: str = "./fraud_results"
    ) -> StreamingQuery:
        """
        Save fraud detection results to local storage
        """
        logger.info(f"Saving fraud results to {output_path}")

        return (
            fraud_transactions.writeStream.outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{output_path}_checkpoint")
            .trigger(processingTime="30 seconds")
            .start()
        )

    @staticmethod
    def join_transactions_with_user_activity(
            transactions_parsed: DataFrame,
            user_activity_parsed: DataFrame
    ) -> DataFrame:
        """
        Task 5: Stream-Stream Join
        Join transactions and user activity streams by user_id with time-range conditions.
        Both streams must have watermarks applied before joining.
        Time range: transaction timestamp ± 5 minutes from user activity timestamp
        """
        logger.info("Performing stream-stream join with time-range conditions...")
        transactions_with_alias = transactions_parsed.select(
            col("transaction_id"),
            col("user_id").alias("txn_user_id"),
            col("amount"),
            col("merchant"),
            col("timestamp").alias("txn_timestamp"),
            col("is_fraud")
        )

        user_activity_with_alias = user_activity_parsed.select(
            col("event_id"),
            col("user_id").alias("activity_user_id"),
            col("event_type"),
            col("device"),
            col("browser"),
            col("timestamp").alias("activity_timestamp")
        )

        join_condition = (
                (col("txn_user_id") == col("activity_user_id")) &
                (col("txn_timestamp") >= col("activity_timestamp") - expr("INTERVAL 5 MINUTES")) &
                (col("txn_timestamp") <= col("activity_timestamp") + expr("INTERVAL 5 MINUTES"))
        )

        return transactions_with_alias.join(
            user_activity_with_alias, join_condition, "inner"
        ).select(
            col("transaction_id"),
            col("txn_user_id").alias("user_id"),
            col("amount"),
            col("merchant"),
            col("txn_timestamp").alias("transaction_timestamp"),
            col("is_fraud"),
            col("event_id"),
            col("event_type"),
            col("device"),
            col("browser"),
            col("activity_timestamp"),
            (
                col("txn_timestamp").cast("long")
                - col("activity_timestamp").cast("long")
            ).alias("time_diff_seconds"),
        )

    @staticmethod
    def save_joined_results_locally(
            joined_df: DataFrame,
            output_path: str = "./joined_streams_results"
    ) -> StreamingQuery:
        """
        Task 5: Save joined stream results to local storage
        """
        logger.info(f"Saving joined stream results to {output_path}")

        return (
            joined_df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", output_path)
            .option("checkpointLocation", f"{output_path}_checkpoint")
            .trigger(processingTime="30 seconds")
            .start()
        )

    def main(self):
        """
        Demonstration of all four tasks
        """
        logger.info("Starting Spark Structured Streaming Demo (Tasks 1-4)...")

        # Task 1: Reading from Kafka
        streams = self.read_kafka_streams()
        transactions_raw = streams["transactions_raw"]
        user_activity_raw = streams["user_activity_raw"]

        # Task 2: Transaction Processing
        transactions_parsed = self.parse_transactions_json_with_watermark(transactions_raw)
        user_activity_parsed = self.parse_user_activity_json_with_watermark(user_activity_raw)

        # Task 3: User Activity Analysis with Sliding Windows
        logger.info("\n=== Task 3: User Activity Analysis ===")

        windowed_activity = self.analyze_user_activity_with_windows(
            user_activity_parsed,
            window_duration="10 minutes",
            slide_duration="5 minutes"
        )

        event_summary = self.analyze_event_type_summary(
            user_activity_parsed,
            window_duration="10 minutes"
        )

        # Task 4: Fraud Detection
        logger.info("\n=== Task 4: Fraud Detection ===")

        fraud_transactions = self.detect_fraud_transactions(transactions_parsed)

        # Task 5: Stream-Stream Join
        logger.info("\n=== Task 5: Stream-Stream Join ===")
        joined_streams = self.join_transactions_with_user_activity(
            transactions_parsed,
            user_activity_parsed
        )

        # Show schemas for verification
        logger.info("\nTransaction DataFrame Schema:")
        transactions_parsed.printSchema()

        logger.info("\nUser Activity DataFrame Schema:")
        user_activity_parsed.printSchema()

        logger.info("\nWindowed Activity Analysis Schema:")
        windowed_activity.printSchema()

        logger.info("\nEvent Summary Schema:")
        event_summary.printSchema()

        logger.info("\nFraud Detection Schema:")
        fraud_transactions.printSchema()

        logger.info("\nJoined Streams Schema:")
        joined_streams.printSchema()

        # Output results to console for verification
        logger.info("Starting console output for transactions...")
        transactions_query = (
            transactions_parsed
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 10)
            .trigger(processingTime="10 seconds")
            .start()
        )

        logger.info("Starting console output for user activities...")
        activities_query = (
            user_activity_parsed
            .writeStream
            .outputMode("append")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 10)
            .trigger(processingTime="10 seconds")
            .start()
        )

        # Task 3: Windowed analysis
        logger.info("\n=== User Activity with Sliding Windows ===")
        windowed_query = (
            windowed_activity
            .writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 15)
            .trigger(processingTime="15 seconds")
            .start()
        )

        logger.info("\n=== Event Type Summary ===")
        summary_query = (
            event_summary
            .writeStream
            .outputMode("complete")
            .format("console")
            .option("truncate", "false")
            .option("numRows", 15)
            .trigger(processingTime="15 seconds")
            .start()
        )

        # Task 4: Save fraud results locally
        fraud_save_query = self.save_fraud_results_locally(
            fraud_transactions,
            output_path="./fraud_detection_results"
        )

        # Task 5: Save joined results locally
        joined_save_query = self.save_joined_results_locally(
            joined_streams,
            output_path="./joined_streams_results"
        )

        return [
            transactions_query,
            activities_query,
            windowed_query,
            summary_query,
            fraud_save_query,
            joined_save_query
        ]

    def stop(self):
        """Stop Spark session"""
        self.spark.stop()



if __name__ == "__main__":
    queries = []
    processor = None

    try:
        processor = SparkJsonSchemaStreamProcessor()
        queries = processor.main()

        logger.info("Streaming started successfully!")
        logger.info("Press Ctrl+C to stop...")

        for query in queries:
            query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("\nStopping streams...")
        for query in queries:
            with contextlib.suppress(Exception):
                query.stop()
    except Exception as e:
        error_msg = str(e)
        logger.info(f"Error: {error_msg}")
    finally:
        for query in queries:
            with contextlib.suppress(Exception):
                query.stop()
        if processor:
            with contextlib.suppress(Exception):
                processor.stop()
        logger.info("Spark session stopped.")
