import logging
import os
from enum import Enum
from glob import glob
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

BASE_DIR = Path(__file__).resolve().parent.parent


class TaxiTypeEnum(Enum):
    GREEN = 'green'
    YELLOW = 'yellow'


class NYCTaxiProcessor:
    def __init__(self, spark: SparkSession, data_path: str = "source_data/home_work_data"):
        """
        Initialize the NYC Taxi Data Processor.

        :param spark: Spark session object
        :param data_path: Base path to the local data directory
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self.data_path = os.path.join(BASE_DIR, data_path)

        # Postgre
        self.postgres_properties = {
            "user": os.getenv("POSTGRES_USER", "dbt_user"),
            "password": os.getenv("POSTGRES_PASSWORD", "12345678"),
            "driver": "org.postgresql.Driver"
        }
        self.postgres_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://dbt_hw_20_host:5432/dbt_hw_20")

        # Tables
        self.yellow_table = "yellow_taxi_stage"
        self.green_table = "green_taxi_stage"
        self.zone_table = "taxi_lookup_zone_stage"

    def _get_taxi_schema(self, taxi_type: TaxiTypeEnum) -> StructType:
        """Get schema for taxi data based on type."""
        common_fields = [
            StructField('VendorID', LongType(), True),
            StructField('passenger_count', DoubleType(), True),
            StructField('trip_distance', DoubleType(), True),
            StructField('RatecodeID', LongType(), True),
            StructField('store_and_fwd_flag', StringType(), True),
            StructField('PULocationID', LongType(), True),
            StructField('DOLocationID', LongType(), True),
            StructField('payment_type', LongType(), True),
            StructField('fare_amount', DoubleType(), True),
            StructField('extra', DoubleType(), True),
            StructField('mta_tax', DoubleType(), True),
            StructField('tip_amount', DoubleType(), True),
            StructField('tolls_amount', DoubleType(), True),
            StructField('improvement_surcharge', DoubleType(), True),
            StructField('total_amount', DoubleType(), True),
            StructField('congestion_surcharge', DoubleType(), True)
        ]

        if taxi_type == TaxiTypeEnum.YELLOW:
            specific_fields = [
                StructField('tpep_pickup_datetime', TimestampType(), True),
                StructField('tpep_dropoff_datetime', TimestampType(), True),
                StructField("airport_fee", DoubleType(), True),
            ]
        else:  # GREEN
            specific_fields = [
                StructField('lpep_pickup_datetime', TimestampType(), True),
                StructField('lpep_dropoff_datetime', TimestampType(), True),
                StructField('ehail_fee', DoubleType(), True),
                StructField('trip_type', LongType(), True)
            ]

        return StructType(specific_fields + common_fields)

    def _get_zone_lookup_schema(self) -> StructType:
        """Get schema for zone lookup data."""
        return StructType([
            StructField('LocationID', LongType(), True),
            StructField('Borough', StringType(), True),
            StructField('Zone', StringType(), True),
            StructField('service_zone', StringType(), True)
        ])

    def normalize_parquet_file(self, file_path: str, taxi_type: TaxiTypeEnum, output_path: str) -> bool:
        """Normalize a single parquet file to consistent schema."""
        try:
            self.logger.info(f"Normalizing parquet file: {file_path}")
            df = self.spark.read.parquet(file_path)

            target_schema = self._get_taxi_schema(taxi_type)

            select_expressions = []

            for field in target_schema.fields:
                field_name = field.name
                field_type = field.dataType

                if field_name in df.columns:
                    if isinstance(field_type, DoubleType | DecimalType):
                        expr_value = when(col(field_name).isNotNull(), col(field_name).cast(DoubleType())) \
                            .otherwise(lit(None).cast(DoubleType())).alias(field_name)
                    elif isinstance(field_type, LongType):
                        expr_value = when(col(field_name).isNotNull(), col(field_name).cast(LongType())) \
                            .otherwise(lit(None).cast(LongType())).alias(field_name)
                    elif isinstance(field_type, StringType):
                        expr_value = when(col(field_name).isNotNull(), col(field_name).cast(StringType())) \
                            .otherwise(lit(None).cast(StringType())).alias(field_name)
                    elif isinstance(field_type, TimestampType):
                        expr_value = when(col(field_name).isNotNull(), col(field_name).cast(TimestampType())) \
                            .otherwise(lit(None).cast(TimestampType())).alias(field_name)
                    else:
                        expr_value = col(field_name)
                elif isinstance(field_type, DoubleType):
                    expr_value = lit(None).cast(DoubleType()).alias(field_name)
                elif isinstance(field_type, LongType):
                    expr_value = lit(None).cast(LongType()).alias(field_name)
                elif isinstance(field_type, StringType):
                    expr_value = lit(None).cast(StringType()).alias(field_name)
                elif isinstance(field_type, TimestampType):
                    expr_value = lit(None).cast(TimestampType()).alias(field_name)
                else:
                    expr_value = lit(None).cast(field_type).alias(field_name)

                select_expressions.append(expr_value)

            final_df = df.select(*select_expressions)

            final_df.coalesce(1).write.mode("overwrite").parquet(output_path)

            record_count = final_df.count()
            self.logger.info(f"Successfully normalized {record_count:,} records from {file_path} to {output_path}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to normalize file {file_path}: {e}")
            return False

    def normalize_taxi_files(self, taxi_type: TaxiTypeEnum) -> str:
        """Normalize all parquet files for a specific taxi type."""
        source_folder = os.path.join(self.data_path, f"{taxi_type.value}_taxi")
        normalized_folder = os.path.join(self.data_path, f"{taxi_type.value}_taxi_normalized")

        self.logger.info(f"Starting normalization for {taxi_type.value} taxi files")
        self.logger.info(f"Source: {source_folder}")
        self.logger.info(f"Target: {normalized_folder}")

        os.makedirs(normalized_folder, exist_ok=True)

        parquet_patterns = [
            os.path.join(source_folder, "*.parquet"),
            os.path.join(source_folder, "*.parq"),
            os.path.join(source_folder, "**", "*.parquet"),
            os.path.join(source_folder, "**", "*.parq"),
        ]

        parquet_files = []
        for pat in parquet_patterns:
            parquet_files.extend(glob(pat, recursive=True))

        if not parquet_files:
            raise RuntimeError(f"No Parquet files found under {source_folder}")

        self.logger.info(f"Found {len(parquet_files)} parquet files to normalize")

        success_count = 0
        for i, file_path in enumerate(parquet_files):
            # rel_path = os.path.relpath(file_path, source_folder)
            output_file = os.path.join(normalized_folder, f"normalized_{i:04d}.parquet")

            if self.normalize_parquet_file(file_path, taxi_type, output_file):
                success_count += 1

        self.logger.info(f"Normalized {success_count}/{len(parquet_files)} files successfully")

        if success_count == 0:
            raise RuntimeError("Failed to normalize any files")

        return normalized_folder

    def read_taxi_data(self, taxi_type: TaxiTypeEnum, use_normalized: bool = True) -> DataFrame:
        """Read taxi data from local parquet files (recursively) with schema evolution support."""
        if use_normalized:
            folder_path = os.path.join(self.data_path, f"{taxi_type.value}_taxi_normalized")
        else:
            folder_path = os.path.join(self.data_path, f"{taxi_type.value}_taxi")

        self.logger.info(f"Reading {taxi_type.value} taxi data from {folder_path}")

        try:
            if not os.path.isdir(folder_path):
                if use_normalized:
                    self.logger.warning(f"Normalized folder not found: {folder_path}. Using original files.")
                    return self.read_taxi_data(taxi_type, use_normalized=False)
                else:
                    raise FileNotFoundError(f"Folder does not exist: {folder_path}")

            parquet_patterns = [
                os.path.join(folder_path, "*.parquet"),
                os.path.join(folder_path, "*.parq"),
                os.path.join(folder_path, "**", "*.parquet"),
                os.path.join(folder_path, "**", "*.parq"),
            ]
            parquet_files = []
            for pat in parquet_patterns:
                parquet_files.extend(glob(pat, recursive=True))

            if not parquet_files:
                if use_normalized:
                    self.logger.warning("No normalized parquet files found. Using original files.")
                    return self.read_taxi_data(taxi_type, use_normalized=False)
                else:
                    raise RuntimeError(
                        f"No Parquet files found under {folder_path}. "
                        f"Make sure data is present (e.g., {folder_path}/part-*.parquet)."
                    )

            if use_normalized:
                df = (
                    self.spark.read
                    .option("recursiveFileLookup", "true")
                    .schema(self._get_taxi_schema(taxi_type))
                    .parquet(*parquet_files)
                )
            else:
                df = (
                    self.spark.read
                    .option("recursiveFileLookup", "true")
                    .option("spark.sql.parquet.enableVectorizedReader", "false")
                    .parquet(*parquet_files)
                )

                if "passenger_count" in df.columns:
                    df = df.withColumn("passenger_count",
                                       when(col("passenger_count").isNotNull(),
                                            col("passenger_count").cast(DoubleType()))
                                       .otherwise(lit(None).cast(DoubleType())))

                if "RatecodeID" in df.columns:
                    df = df.withColumn("RatecodeID",
                                       when(col("RatecodeID").isNotNull(),
                                            col("RatecodeID").cast(LongType()))
                                       .otherwise(lit(None).cast(LongType())))

                if taxi_type == TaxiTypeEnum.YELLOW and "airport_fee" in df.columns:
                    df = df.withColumn("airport_fee",
                                       when(col("airport_fee").isNotNull(),
                                            col("airport_fee").cast(DoubleType()))
                                       .otherwise(lit(None).cast(DoubleType())))

            record_count = df.count()
            self.logger.info(f"Successfully read {record_count:,} {taxi_type.value} taxi records")
            return df

        except Exception as e:
            self.logger.error(f"Failed to read {taxi_type.value} taxi data: {e}")
            raise

    def read_zone_lookup(self) -> DataFrame:
        """Read zone lookup data from CSV file."""
        zone_path = os.path.join(self.data_path, "taxi_zone_lookup.csv")

        self.logger.info(f"Reading zone lookup data from {zone_path}")

        try:
            df = self.spark.read.csv(
                zone_path,
                header=True,
                schema=self._get_zone_lookup_schema()
            )
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count} zone lookup records")
            return df
        except Exception as e:
            self.logger.error(f"Failed to read zone lookup data: {e}")
            raise

    def clean_and_filter_data(self, df: DataFrame, taxi_type: TaxiTypeEnum) -> DataFrame:
        """Apply data quality filters and cleaning."""
        self.logger.info(f"Applying filters and cleaning to {taxi_type.value} taxi data")

        initial_count = df.count()

        # Filter based on task requirements
        filtered_df = df.filter(
            (col('trip_distance') > 0) &
            (col('fare_amount') > 0) &
            (col('fare_amount') <= 500) &
            (col('passenger_count').isNotNull()) &
            (col('passenger_count') <= 6) &
            (col('total_amount') > 0) &
            (col('PULocationID').isNotNull()) &
            (col('DOLocationID').isNotNull())
        )

        final_count = filtered_df.count()
        filtered_out = initial_count - final_count

        self.logger.info(f"Filtering results for {taxi_type.value}:")
        self.logger.info(f"  Initial records: {initial_count:,}")
        self.logger.info(f"  Final records: {final_count:,}")
        self.logger.info(f"  Filtered out: {filtered_out:,} ({(filtered_out / initial_count * 100):.2f}%)")

        return filtered_df

    def standardize_taxi_data(self, df: DataFrame, taxi_type: TaxiTypeEnum) -> DataFrame:
        """Standardize taxi data to common schema and add features."""
        self.logger.info(f"Standardizing {taxi_type.value} taxi data")

        if taxi_type == TaxiTypeEnum.YELLOW:
            pickup_col = 'tpep_pickup_datetime'
            dropoff_col = 'tpep_dropoff_datetime'

            standardized_df = df.select(
                col('VendorID').alias('vendor_id'),
                col(pickup_col).alias('pickup_datetime'),
                col(dropoff_col).alias('dropoff_datetime'),
                col('passenger_count').cast(LongType()),
                col('trip_distance'),
                col('RatecodeID').cast(LongType()).alias('rate_code_id'),
                col('store_and_fwd_flag'),
                col('PULocationID').alias('pu_location_id'),
                col('DOLocationID').alias('do_location_id'),
                col('payment_type'),
                col('fare_amount'),
                col('extra'),
                col('mta_tax'),
                col('tip_amount'),
                col('tolls_amount'),
                lit(None).cast(DoubleType()).alias('ehail_fee'),
                col('improvement_surcharge'),
                col('total_amount'),
                lit(None).cast(LongType()).alias('trip_type'),
                col('congestion_surcharge'),
                col('airport_fee'),
                lit('yellow').alias('taxi_type')
            )
        else:
            pickup_col = 'lpep_pickup_datetime'
            dropoff_col = 'lpep_dropoff_datetime'

            standardized_df = df.select(
                col('VendorID').alias('vendor_id'),
                col(pickup_col).alias('pickup_datetime'),
                col(dropoff_col).alias('dropoff_datetime'),
                col('passenger_count').cast(LongType()),
                col('trip_distance'),
                col('RatecodeID').cast(LongType()).alias('rate_code_id'),
                col('store_and_fwd_flag'),
                col('PULocationID').alias('pu_location_id'),
                col('DOLocationID').alias('do_location_id'),
                col('payment_type'),
                col('fare_amount'),
                col('extra'),
                col('mta_tax'),
                col('tip_amount'),
                col('tolls_amount'),
                col('ehail_fee'),
                col('improvement_surcharge'),
                col('total_amount'),
                col('trip_type'),
                col('congestion_surcharge'),
                lit(None).cast(DoubleType()).alias('airport_fee'),
                lit('green').alias('taxi_type')
            )

        enhanced_df = standardized_df.withColumn(
            'trip_duration_minutes',
            (unix_timestamp(col('dropoff_datetime')) - unix_timestamp(col('pickup_datetime'))) / 60
        ).withColumn(
            'pickup_hour',
            hour(col('pickup_datetime'))
        ).withColumn(
            'pickup_day_of_week',
            dayofweek(col('pickup_datetime'))
        ).withColumn(
            'pickup_date',
            to_date(col('pickup_datetime'))
        )

        enhanced_df = enhanced_df.cache()

        return enhanced_df.filter(
            (col('trip_duration_minutes') > 0)
            & (col('trip_duration_minutes') <= 1440)
        )

    def enrich_with_zones(self, trips_df: DataFrame, zones_df: DataFrame) -> DataFrame:
        """Enrich trips data with pickup and dropoff zone information."""
        self.logger.info("Enriching trips with zone information")

        pickup_zones = zones_df.select(
            col('LocationID').alias('pickup_location_id'),
            col('Borough').alias('pickup_borough'),
            col('Zone').alias('pickup_zone'),
            col('service_zone').alias('pickup_service_zone')
        )

        trips_with_pickup = trips_df.join(
            broadcast(pickup_zones),
            trips_df.pu_location_id == pickup_zones.pickup_location_id,
            'left'
        ).drop('pickup_location_id')

        dropoff_zones = zones_df.select(
            col('LocationID').alias('dropoff_location_id'),
            col('Borough').alias('dropoff_borough'),
            col('Zone').alias('dropoff_zone'),
            col('service_zone').alias('dropoff_service_zone')
        )

        enriched_df = trips_with_pickup.join(
            dropoff_zones,
            trips_with_pickup.do_location_id == dropoff_zones.dropoff_location_id,
            'left'
        ).drop('dropoff_location_id')

        enriched_df = enriched_df.cache()

        return enriched_df

    def save_to_postgres(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        """Save DataFrame to PostgreSQL."""
        self.logger.info(f"Saving data to PostgreSQL table: {table_name}")

        try:
            df.write \
                .mode(mode) \
                .jdbc(
                url=self.postgres_url,
                table=table_name,
                properties=self.postgres_properties
            )

            record_count = df.count()
            self.logger.info(f"Successfully saved {record_count:,} records to {table_name}")

        except Exception as e:
            self.logger.error(f"Failed to save data to {table_name}: {e}")
            raise

    def process_taxi_type(self, taxi_type: TaxiTypeEnum, zones_df: DataFrame, use_normalized: bool = True) -> DataFrame:
        """Process data for a specific taxi type."""
        self.logger.info(f"Processing {taxi_type.value} taxi data")

        # Read raw data (normalized or original)
        raw_df = self.read_taxi_data(taxi_type, use_normalized=use_normalized)

        # Clean and filter
        cleaned_df = self.clean_and_filter_data(raw_df, taxi_type)

        # Standardize schema and add features
        standardized_df = self.standardize_taxi_data(cleaned_df, taxi_type)

        # Enrich with zone information
        enriched_df = self.enrich_with_zones(standardized_df, zones_df)

        # Save to PostgreSQL
        table_name = self.yellow_table if taxi_type == TaxiTypeEnum.YELLOW else self.green_table
        self.save_to_postgres(enriched_df, table_name)

        return enriched_df

    def process_all_data(self, normalize_first: bool = True) -> None:
        """Main method to process all taxi data."""
        self.logger.info("Starting NYC Taxi data processing pipeline")

        try:
            # Step 1: Normalize parquet files if requested
            if normalize_first:
                self.logger.info("Step 1: Normalizing parquet files")
                try:
                    yellow_normalized_path = self.normalize_taxi_files(TaxiTypeEnum.YELLOW)
                    self.logger.info(f"Yellow taxi files normalized to: {yellow_normalized_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to normalize yellow taxi files: {e}")
                    self.logger.warning("Will proceed with original files")
                    normalize_first = False

                try:
                    green_normalized_path = self.normalize_taxi_files(TaxiTypeEnum.GREEN)
                    self.logger.info(f"Green taxi files normalized to: {green_normalized_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to normalize green taxi files: {e}")
                    self.logger.warning("Will proceed with original files")
                    normalize_first = False

            # Step 2: Load zone lookup data
            self.logger.info("Step 2: Loading zone lookup data")
            zones_df = self.read_zone_lookup()

            # Save zone lookup to PostgreSQL
            self.save_to_postgres(zones_df, self.zone_table)

            # Step 3: Process yellow taxi data
            self.logger.info("Step 3: Processing yellow taxi data")
            yellow_df = self.process_taxi_type(TaxiTypeEnum.YELLOW, zones_df, use_normalized=normalize_first)

            # Step 4: Process green taxi data
            self.logger.info("Step 4: Processing green taxi data")
            green_df = self.process_taxi_type(TaxiTypeEnum.GREEN, zones_df, use_normalized=normalize_first)

            # Log final statistics
            yellow_count = yellow_df.count()
            green_count = green_df.count()
            total_count = yellow_count + green_count

            self.logger.info("Pipeline completed successfully!")
            self.logger.info("Final statistics:")
            self.logger.info(f"  Yellow taxi trips: {yellow_count:,}")
            self.logger.info(f"  Green taxi trips: {green_count:,}")
            self.logger.info(f"  Total trips processed: {total_count:,}")

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NYC_Taxi_Data_Processing")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


def main():
    """Main execution function."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    spark = None
    try:
        spark = create_spark_session()
        processor = NYCTaxiProcessor(spark)
        processor.process_all_data()

        logger.info("NYC Taxi data processing completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
