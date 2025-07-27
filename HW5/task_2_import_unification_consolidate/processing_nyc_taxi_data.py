from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timezone
from enum import Enum
import logging


class TaxiTypeEnum(Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"

def is_databricks_environment():
    """Check if running in Databricks environment."""
    try:
        import pyspark.dbutils
        return True
    except ImportError:
        return False


class TaxiDataProcessor:
    """
    A class to process NYC taxi data, combining yellow and green taxi datasets,
    adding time-based features, and joining with zone lookup data.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the TaxiDataProcessor.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # Configuration
        self.S3_TEMP_BUCKET = "zhogolev-pv-temp-files"
        self.YELLOW_TEMP_FOLDER = f"s3://{self.S3_TEMP_BUCKET}/temp/yellow_taxi_combined_da284725/"
        self.GREEN_TEMP_FOLDER = f"s3://{self.S3_TEMP_BUCKET}/temp/green_taxi_combined_da284725/"
        self.S3_BUCKET_NAME = "robot-dreams-source-data"
        self.ZONE_LOOKUP_PATH = f"s3://{self.S3_BUCKET_NAME}/Lecture_3/nyc_taxi/taxi_zone_lookup.csv"
        self.CATALOG_BASE_BUCKET = "s3://zhoholiev-pavlo-databricks-s3/catalogs"
        self.current_date = datetime.now(timezone.utc).date().isoformat()

        # Catalog configuration
        self.CATALOG_NAME = "pavlo_zhoholiev_nyc_catalog"
        self.SCHEMA_NAME = "trips_schema"
        self.TABLE_NAME = "trips_with_all_zones"

        # Schemas
        self.green_processed_schema = self._get_green_schema()
        self.yellow_processed_schema = self._get_yellow_schema()
        self.zone_lookup_schema = self._get_zone_lookup_schema()

    def _get_green_schema(self) -> StructType:
        """Get the schema for green taxi data."""
        return StructType([
            StructField('VendorID', LongType(), True),
            StructField('lpep_pickup_datetime', TimestampNTZType(), True),
            StructField('lpep_dropoff_datetime', TimestampNTZType(), True),
            StructField('store_and_fwd_flag', StringType(), True),
            StructField('RatecodeID', DoubleType(), True),
            StructField('PULocationID', LongType(), True),
            StructField('DOLocationID', LongType(), True),
            StructField('passenger_count', LongType(), True),
            StructField('trip_distance', DoubleType(), True),
            StructField('fare_amount', DoubleType(), True),
            StructField('extra', DoubleType(), True),
            StructField('mta_tax', DoubleType(), True),
            StructField('tip_amount', DoubleType(), True),
            StructField('tolls_amount', DoubleType(), True),
            StructField('ehail_fee', DoubleType(), True),
            StructField('improvement_surcharge', DoubleType(), True),
            StructField('total_amount', DoubleType(), True),
            StructField('payment_type', LongType(), True),
            StructField('trip_type', LongType(), True),
            StructField('congestion_surcharge', DoubleType(), True)
        ])

    def _get_yellow_schema(self) -> StructType:
        """Get the schema for yellow taxi data."""
        return StructType([
            StructField("VendorID", LongType(), True),
            StructField("tpep_pickup_datetime", TimestampNTZType(), True),
            StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
            StructField("passenger_count", LongType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("payment_type", LongType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True)
        ])

    def _get_zone_lookup_schema(self) -> StructType:
        """Get the schema for zone lookup data."""
        return StructType([
            StructField("LocationID", LongType(), True),
            StructField("Borough", StringType(), True),
            StructField("Zone", StringType(), True),
            StructField("service_zone", StringType(), True),
        ])

    def verify_processed_data(self, s3_folder: str, taxi_type: TaxiTypeEnum, schema: StructType):
        """
        Safely verify and read processed data using sampling.

        Args:
            s3_folder: S3 path to the data
            taxi_type: Type of taxi data (GREEN or YELLOW)
            schema: Schema for the data

        Returns:
            DataFrame: Loaded data
        """
        try:
            self.logger.info(f"Reading {taxi_type.value} processed data from {s3_folder}")
            df = self.spark.read.schema(schema).option("recursiveFileLookup", "true").parquet(s3_folder)

            sample_fraction = 0.005
            sample_df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
            sample_count = sample_df.count()
            estimated_total = int(sample_count / sample_fraction) if sample_count > 0 else 0

            self.logger.info(f"Columns: {len(df.columns)}")
            self.logger.info(f"Estimated total records: ~{estimated_total:,}")

            return df

        except Exception as e:
            self.logger.error(f"Verification failed for {taxi_type.value}: {e}")
            raise

    def standardize_yellow_taxi(self, df):
        """Standardize yellow taxi DataFrame to common schema."""
        return df.select(
            col("VendorID"),
            col("tpep_pickup_datetime").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            col("passenger_count"),
            col("trip_distance"),
            col("RatecodeID"),
            col("store_and_fwd_flag"),
            col("PULocationID"),
            col("DOLocationID"),
            col("payment_type"),
            col("fare_amount"),
            col("extra"),
            col("mta_tax"),
            col("tip_amount"),
            col("tolls_amount"),
            lit(None).cast("double").alias("ehail_fee"),
            col("improvement_surcharge"),
            col("total_amount"),
            lit(None).cast("long").alias("trip_type"),
            col("congestion_surcharge"),
            col("airport_fee"),
            lit("yellow").alias("taxi_type")
        )

    def standardize_green_taxi(self, df):
        """Standardize green taxi DataFrame to common schema."""
        return df.select(
            col("VendorID"),
            col("lpep_pickup_datetime").alias("pickup_datetime"),
            col("lpep_dropoff_datetime").alias("dropoff_datetime"),
            col("passenger_count"),
            col("trip_distance"),
            col("RatecodeID"),
            col("store_and_fwd_flag"),
            col("PULocationID"),
            col("DOLocationID"),
            col("payment_type"),
            col("fare_amount"),
            col("extra"),
            col("mta_tax"),
            col("tip_amount"),
            col("tolls_amount"),
            col("ehail_fee"),
            col("improvement_surcharge"),
            col("total_amount"),
            col("trip_type"),
            col("congestion_surcharge"),
            lit(None).cast("double").alias("airport_fee"),
            lit("green").alias("taxi_type")
        )

    def add_time_features(self, df):
        """Add time-based features to the DataFrame."""
        return df.withColumn(
            "trip_duration_minutes",
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
        ).withColumn(
            "pickup_hour", hour(col("pickup_datetime"))
        ).withColumn(
            "pickup_day_of_week", dayofweek(col("pickup_datetime"))
        ).withColumn(
            "duration_min",
            (unix_timestamp(col("dropoff_datetime")) - unix_timestamp(col("pickup_datetime"))) / 60
        )

    def filter_trips(self, df):
        """Apply data quality filters to the trips."""
        return df.filter(
            (col("trip_distance") >= 0.1) &
            (col("fare_amount") >= 2.0) &
            (col("trip_duration_minutes") >= 1.0)
        )

    def load_zone_lookup(self, zone_lookup_path: str):
        """Load zone lookup data."""
        return self.spark.read.csv(
            zone_lookup_path,
            header=True,
            schema=self.zone_lookup_schema
        )

    @staticmethod
    def join_zones(trips_df, zone_lookup_df):
        """Join pickup and dropoff zone information."""
        pickup_zones = zone_lookup_df.select(
            col("LocationID").alias("pickup_location_id"),
            col("Zone").alias("pickup_zone"),
            col("Borough").alias("pickup_borough"),
            col("service_zone").alias("pickup_service_zone")
        )

        trips_with_pickup = trips_df.join(
            broadcast(pickup_zones),
            trips_df.PULocationID == pickup_zones.pickup_location_id,
            "left"
        ).drop("pickup_location_id")

        dropoff_zones = zone_lookup_df.select(
            col("LocationID").alias("dropoff_location_id"),
            col("Zone").alias("dropoff_zone"),
            col("Borough").alias("dropoff_borough"),
            col("service_zone").alias("dropoff_service_zone")
        )
        return trips_with_pickup.join(
            dropoff_zones,
            trips_with_pickup.DOLocationID == dropoff_zones.dropoff_location_id,
            "left",
        ).drop("dropoff_location_id")

    def save_to_catalog(self, df, mode="overwrite"):
        """Save DataFrame to Unity Catalog."""
        table_path = f"{self.CATALOG_NAME}.{self.SCHEMA_NAME}.{self.TABLE_NAME}"

        self.logger.info(f"Saving data to catalog table: {table_path}")

        df.write \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .saveAsTable(table_path)

        self.logger.info(f"Data successfully saved to {table_path}")

    def create_schema_in_catalog(self):
        """
        Create a schema in Unity Catalog if it does not exist.
        This method is called before creating the table to ensure the schema is ready.
        """
        self.logger.info(f"Creating schema {self.CATALOG_NAME}.{self.SCHEMA_NAME} if it does not exist.")
        self.spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {self.CATALOG_NAME}.{self.SCHEMA_NAME}
        """)

    def create_table_in_catalog_schema(self):
        """
        Create a table in Unity Catalog if it does not exist.
        This method is called before saving the DataFrame to ensure the table is ready.
        """
        self.logger.info(
            f"Creating table {self.CATALOG_NAME}.{self.SCHEMA_NAME}.{self.TABLE_NAME} if it does not exist.")

        # Create table with complete schema matching the DataFrame structure
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.CATALOG_NAME}.{self.SCHEMA_NAME}.{self.TABLE_NAME} (
                VendorID LONG,
                pickup_datetime TIMESTAMP_NTZ,
                dropoff_datetime TIMESTAMP_NTZ,
                passenger_count LONG,
                trip_distance DOUBLE,
                RatecodeID DOUBLE,
                store_and_fwd_flag STRING,
                PULocationID LONG,
                DOLocationID LONG,
                payment_type LONG,
                fare_amount DOUBLE,
                extra DOUBLE,
                mta_tax DOUBLE,
                tip_amount DOUBLE,
                tolls_amount DOUBLE,
                ehail_fee DOUBLE,
                improvement_surcharge DOUBLE,
                total_amount DOUBLE,
                trip_type LONG,
                congestion_surcharge DOUBLE,
                airport_fee DOUBLE,
                taxi_type STRING NOT NULL,
                trip_duration_minutes DOUBLE,
                pickup_hour INT,
                pickup_day_of_week INT,
                duration_min DOUBLE,
                pickup_zone STRING,
                pickup_borough STRING,
                pickup_service_zone STRING,
                dropoff_zone STRING,
                dropoff_borough STRING,
                dropoff_service_zone STRING
            )
            USING DELTA
            LOCATION '{self.CATALOG_BASE_BUCKET}/{self.CATALOG_NAME}/{self.SCHEMA_NAME}/{self.TABLE_NAME}'
        """)

    def process_taxi_data(self):
        """
        Main method to process taxi data end-to-end.

        Returns:
            DataFrame: Final processed DataFrame
        """
        self.logger.info("Starting taxi data processing...")

        # Step 1: Read raw data
        self.logger.info("Step 1: Reading processed taxi data...")
        yellow_df = self.verify_processed_data(
            self.YELLOW_TEMP_FOLDER,
            TaxiTypeEnum.YELLOW,
            self.yellow_processed_schema
        )
        green_df = self.verify_processed_data(
            self.GREEN_TEMP_FOLDER,
            TaxiTypeEnum.GREEN,
            self.green_processed_schema
        )

        # Step 2: Standardize schemas
        self.logger.info("Step 2: Standardizing schemas...")
        yellow_standardized = self.standardize_yellow_taxi(yellow_df)
        green_standardized = self.standardize_green_taxi(green_df)

        # Step 3: Union datasets
        self.logger.info("Step 3: Combining datasets...")
        combined_df = yellow_standardized.union(green_standardized)

        # Step 4: Add time features
        self.logger.info("Step 4: Adding time-based features...")
        enhanced_df = self.add_time_features(combined_df)

        # Step 5: Filter data
        self.logger.info("Step 5: Applying data quality filters...")
        initial_count = enhanced_df.count()
        filtered_df = self.filter_trips(enhanced_df)
        final_count = filtered_df.count()
        removed_count = initial_count - final_count

        self.logger.info(f"Filtering results:")
        self.logger.info(f"  Total trips: {final_count:,}")
        self.logger.info(f"  Removed trips: {removed_count:,}")
        self.logger.info(f"  Removal rate: {(removed_count / initial_count * 100):.2f}%")

        # Step 6: Load zone lookup
        self.logger.info("Step 6: Loading zone lookup data...")
        zone_lookup_df = self.load_zone_lookup(self.ZONE_LOOKUP_PATH)

        # Step 7: Join zones
        self.logger.info("Step 7: Joining zone information...")
        final_df = self.join_zones(filtered_df, zone_lookup_df)

        self.logger.info(f"Final DataFrame columns: {final_df.columns}")
        # Step 8: Create schema and table in Unity Catalog
        self.logger.info(f"Step 8: Creating schema in Unity Catalog if not exists: {self.CATALOG_NAME}.{self.SCHEMA_NAME}")
        self.create_schema_in_catalog()
        # Step 9: Create table in Unity Catalog
        self.logger.info(f"Step 9: Crate table in Unity Catalog if not exists: {self.CATALOG_NAME}.{self.SCHEMA_NAME}.{self.TABLE_NAME}")
        self.create_table_in_catalog_schema()

        # Step 10: Save to catalog
        self.logger.info("Step 10: Saving to Unity Catalog...")
        self.save_to_catalog(final_df)

        self.logger.info("Taxi data processing completed successfully!")

        # Log final statistics
        yellow_count = final_df.filter(col('taxi_type') == 'yellow').count()
        green_count = final_df.filter(col('taxi_type') == 'green').count()

        self.logger.info("Final dataset statistics:")
        self.logger.info(f"  Yellow trips: {yellow_count:,}")
        self.logger.info(f"  Green trips: {green_count:,}")
        self.logger.info(f"  Total trips: {final_df.count():,}")
        self.logger.info(f"  Total columns: {len(final_df.columns)}")

        return final_df


def main():
    """Main execution function."""
    spark = SparkSession.builder \
        .appName("TaxiDataProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        processor = TaxiDataProcessor(spark)
        processor.process_taxi_data()

        logger.info("Pipeline execution completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise e
    finally:
        if not is_databricks_environment():
            try:
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")



if __name__ == "__main__":
    main()