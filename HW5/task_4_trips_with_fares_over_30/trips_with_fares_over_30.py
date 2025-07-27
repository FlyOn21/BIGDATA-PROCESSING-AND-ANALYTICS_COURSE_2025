from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timezone
from typing import Optional, List
import logging

def is_databricks_environment():
    """Check if running in Databricks environment."""
    try:
        import pyspark.dbutils
        return True
    except ImportError:
        return False


class WeeklyZoneAnalysisProcessor:
    """
    A class to process weekly zone analysis for NYC taxi trip data.
    Analyzes trips with fares over $30 by day of week and pickup zone.
    """

    def __init__(self, spark: SparkSession):
        """
        Initialize the WeeklyZoneAnalysisProcessor.

        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # Configuration
        self.CATALOG_BASE_BUCKET = "s3://zhoholiev-pavlo-databricks-s3/catalogs"
        self.current_date = datetime.now(timezone.utc).date().isoformat()

        # High fare threshold
        self.HIGH_FARE_THRESHOLD = 30.0

        # Day names mapping
        self.day_names = {
            1: "Monday",
            2: "Tuesday",
            3: "Wednesday",
            4: "Thursday",
            5: "Friday",
            6: "Saturday",
            7: "Sunday"
        }

        # Source table configuration (output from TaxiDataProcessor)
        self.SOURCE_CATALOG_NAME = "pavlo_zhoholiev_nyc_catalog"
        self.SOURCE_SCHEMA_NAME = "trips_schema"
        self.SOURCE_TABLE_NAME = "trips_with_all_zones"
        self.source_table_path = f"{self.SOURCE_CATALOG_NAME}.{self.SOURCE_SCHEMA_NAME}.{self.SOURCE_TABLE_NAME}"

        # Target table configuration
        self.TARGET_CATALOG_NAME = "pavlo_zhoholiev_nyc_catalog"
        self.TARGET_SCHEMA_NAME = "analytics_schema"
        self.TARGET_TABLE_NAME = "trips_with_fares_over_30"
        self.target_table_path = f"{self.TARGET_CATALOG_NAME}.{self.TARGET_SCHEMA_NAME}.{self.TARGET_TABLE_NAME}"

        # Weekly zone analysis schema
        self.weekly_zone_schema = self._get_weekly_zone_schema()

    def _get_weekly_zone_schema(self) -> StructType:
        """Get the schema for weekly zone analysis data."""
        return StructType([
            StructField("pickup_zone", StringType(), True),
            StructField("day_of_week", IntegerType(), True),
            StructField("day_name", StringType(), True),
            StructField("trips_count", LongType(), True),
            StructField("high_fare_share", DoubleType(), True),
            StructField("high_fare_count", LongType(), True),
            StructField("avg_total_amount", DoubleType(), True),
            StructField("avg_trip_distance", DoubleType(), True),
            StructField("max_fare", DoubleType(), True),
            StructField("min_fare", DoubleType(), True),
            StructField("processing_date", StringType(), True)
        ])

    def load_source_data(self):
        """
        Load source trip data from Unity Catalog table.

        Returns:
            DataFrame: Source trip data with all zones
        """
        try:
            self.logger.info(f"Loading source data from: {self.source_table_path}")

            # Check if source table exists
            tables = self.spark.sql(f"SHOW TABLES IN {self.SOURCE_CATALOG_NAME}.{self.SOURCE_SCHEMA_NAME}").collect()
            table_names = [row.tableName for row in tables]

            if self.SOURCE_TABLE_NAME not in table_names:
                raise Exception(f"Source table {self.source_table_path} does not exist. "
                                f"Please run TaxiDataProcessor first.")

            df = self.spark.read.table(self.source_table_path)

            # Verify data integrity
            total_count = df.count()
            valid_zones_count = df.filter(col("pickup_zone").isNotNull()).count()

            self.logger.info(f"Source data loaded successfully:")
            self.logger.info(f"  Total records: {total_count:,}")
            self.logger.info(f"  Records with valid pickup zones: {valid_zones_count:,}")
            self.logger.info(f"  Columns: {len(df.columns)}")

            if total_count == 0:
                raise Exception("Source table is empty")

            return df

        except Exception as e:
            self.logger.error(f"Failed to load source data: {e}")
            raise

    def filter_trips_for_day(self, trips_df, day_of_week: int):
        """
        Filter trips for a specific day of week with valid pickup zones.

        Args:
            trips_df: DataFrame with trip data
            day_of_week: Day of week (1-7, where 1=Monday)

        Returns:
            DataFrame: Filtered trips for the specified day
        """
        if day_of_week not in self.day_names.keys():
            raise ValueError(f"day_of_week must be between 1-7, got {day_of_week}")

        filtered_df = trips_df.filter(
            (col("pickup_day_of_week") == day_of_week) &
            (col("pickup_zone").isNotNull())
        )

        return filtered_df

    def calculate_zone_day_stats(self, trips_df, day_of_week: int):
        """
        Calculate zone statistics for a specific day of week.

        Args:
            trips_df: DataFrame with trip data for a specific day
            day_of_week: Day of week (1-7)

        Returns:
            DataFrame: Zone statistics for the day
        """
        self.logger.info(f"Calculating zone stats for {self.day_names[day_of_week]} (day {day_of_week})")

        # Group by pickup zone and calculate aggregations
        zone_day_stats = trips_df.groupBy("pickup_zone").agg(
            count("*").alias("trips_count"),
            sum(when(col("total_amount") > self.HIGH_FARE_THRESHOLD, 1).otherwise(0)).alias("high_fare_count"),
            avg("total_amount").alias("avg_total_amount"),
            avg("trip_distance").alias("avg_trip_distance"),
            max("total_amount").alias("max_fare"),
            min("total_amount").alias("min_fare")
        )

        # Calculate high fare share and add day information
        zone_day_stats_final = zone_day_stats.withColumn(
            "high_fare_share",
            col("high_fare_count") / col("trips_count")
        ).withColumn(
            "day_of_week",
            lit(day_of_week)
        ).withColumn(
            "day_name",
            lit(self.day_names[day_of_week])
        ).withColumn(
            "processing_date",
            lit(self.current_date)
        )
        zone_day_stats_final = zone_day_stats_final.select(
            "pickup_zone",
            "day_of_week",
            "day_name",
            "trips_count",
            "high_fare_share",
            "high_fare_count",
            "avg_total_amount",
            "avg_trip_distance",
            "max_fare",
            "min_fare",
            "processing_date"
        ).orderBy(col("trips_count").desc())

        zone_count = zone_day_stats_final.count()
        total_trips = zone_day_stats_final.agg(sum("trips_count")).collect()[0][0]
        high_fare_trips = zone_day_stats_final.agg(sum("high_fare_count")).collect()[0][0]

        self.logger.info(f"  Zones processed: {zone_count}")
        self.logger.info(f"  Total trips: {total_trips:,}")
        self.logger.info(f"  High fare trips (>${self.HIGH_FARE_THRESHOLD}): {high_fare_trips:,}")

        return zone_day_stats_final

    def get_trips_with_fares_over_30(self, week_day: Optional[int] = None):
        """
        Calculate zone statistics with high fare analysis for specified day(s).

        Args:
            week_day: Specific day of week to process (1-7). If None, processes all days.

        Returns:
            DataFrame: Combined zone statistics for all requested days
        """
        self.logger.info(f"Starting high fare analysis with threshold: ${self.HIGH_FARE_THRESHOLD}")

        # Load source data
        trips_df = self.load_source_data()

        # Determine which days to process
        list_days_int_values = list(self.day_names.keys())
        if week_day is None:
            processed_days_list = list_days_int_values
            self.logger.info("Processing all days of the week")
        else:
            if week_day not in list_days_int_values:
                raise ValueError(f"week_day argument must be between 1-7, got {week_day}")
            processed_days_list = [week_day]
            self.logger.info(f"Processing single day: {self.day_names[week_day]}")

        # Process each day
        days_stats = []
        for day in processed_days_list:
            # Filter trips for this day
            trips_selected_day = self.filter_trips_for_day(trips_df, day)

            # Calculate zone statistics for this day
            zone_day_stats = self.calculate_zone_day_stats(trips_selected_day, day)
            days_stats.append(zone_day_stats)

        # Combine all days
        self.logger.info("Combining statistics for all processed days...")
        result = days_stats[0]
        if len(days_stats) > 1:
            for df in days_stats[1:]:
                result = result.union(df)

        total_records = result.count()
        self.logger.info(f"Combined dataset created with {total_records:,} zone-day combinations")

        return result

    def validate_weekly_analysis(self, weekly_df):
        """
        Validate the weekly zone analysis data.

        Args:
            weekly_df: DataFrame to validate
        """
        self.logger.info("Validating weekly zone analysis data...")

        # Basic statistics
        total_records = weekly_df.count()
        unique_zones = weekly_df.select("pickup_zone").distinct().count()
        days_processed = weekly_df.select("day_of_week").distinct().count()

        self.logger.info(f"Validation results:")
        self.logger.info(f"  Total zone-day combinations: {total_records:,}")
        self.logger.info(f"  Unique zones: {unique_zones:,}")
        self.logger.info(f"  Days processed: {days_processed}")

        # Check data quality
        null_zones = weekly_df.filter(col("pickup_zone").isNull()).count()
        negative_trips = weekly_df.filter(col("trips_count") < 0).count()
        invalid_shares = weekly_df.filter(
            (col("high_fare_share") < 0) | (col("high_fare_share") > 1)
        ).count()

        self.logger.info(f"Data quality check:")
        self.logger.info(f"  Null zones: {null_zones}")
        self.logger.info(f"  Negative trip counts: {negative_trips}")
        self.logger.info(f"  Invalid high fare shares: {invalid_shares}")

        if null_zones > 0 or negative_trips > 0 or invalid_shares > 0:
            self.logger.warning("Data quality issues detected!")

        # Show schema and sample data
        self.logger.info("Weekly analysis schema:")
        weekly_df.printSchema()

        self.logger.info("Sample data for each day:")
        for day_num in self.day_names.keys():
            day_data = weekly_df.filter(col("day_of_week") == day_num)
            day_count = day_data.count()
            if day_count > 0:
                self.logger.info(f"\n{self.day_names[day_num]} - Top 5 zones by trip count:")
                day_data.show(5, truncate=False)

    def create_target_schema_in_catalog(self):
        """Create target schema in Unity Catalog if it doesn't exist."""
        self.logger.info(f"Creating schema {self.TARGET_CATALOG_NAME}.{self.TARGET_SCHEMA_NAME} if it does not exist.")
        self.spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {self.TARGET_CATALOG_NAME}.{self.TARGET_SCHEMA_NAME}
        """)

    def create_target_table_in_catalog(self):
        """Create target table in Unity Catalog if it doesn't exist."""
        self.logger.info(f"Creating table {self.target_table_path} if it does not exist.")

        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.target_table_path} (
                pickup_zone STRING,
                day_of_week INT,
                day_name STRING,
                trips_count BIGINT,
                high_fare_share DOUBLE,
                high_fare_count BIGINT,
                avg_total_amount DOUBLE,
                avg_trip_distance DOUBLE,
                max_fare DOUBLE,
                min_fare DOUBLE,
                processing_date STRING
            )
            USING DELTA
            PARTITIONED BY (processing_date, day_of_week)
            LOCATION '{self.CATALOG_BASE_BUCKET}/{self.TARGET_CATALOG_NAME}/{self.TARGET_SCHEMA_NAME}/{self.TARGET_TABLE_NAME}'
        """)

    def save_to_catalog(self, weekly_df, mode="overwrite"):
        """
        Save weekly zone analysis DataFrame to Unity Catalog.

        Args:
            weekly_df: DataFrame to save
            mode: Write mode ("overwrite", "append", etc.)
        """
        self.logger.info(f"Saving weekly zone analysis to catalog table: {self.target_table_path}")

        weekly_df.write \
            .mode(mode) \
            .option("mergeSchema", "true") \
            .partitionBy("processing_date", "day_of_week") \
            .saveAsTable(self.target_table_path)

        self.logger.info(f"Weekly zone analysis saved successfully to {self.target_table_path}")

        # Verify saved data
        self.logger.info("Verifying saved data...")
        saved_weekly_df = self.spark.read.table(self.target_table_path)
        saved_count = saved_weekly_df.count()
        self.logger.info(f"Saved records count: {saved_count:,}")

        # Show sample of saved data for each day
        self.logger.info("Sample of saved data by day:")
        for day_num in self.day_names.keys():
            day_data = saved_weekly_df.filter(col("day_of_week") == day_num)
            if day_data.count() > 0:
                self.logger.info(f"\n{self.day_names[day_num]} - Top 3 zones:")
                day_data.show(3, truncate=False)

    def process_weekly_zone_analysis(self, week_day: Optional[int] = None, mode="overwrite"):
        """
        Main method to process weekly zone analysis data end-to-end.

        Args:
            week_day: Specific day of week to process (1-7). If None, processes all days.
            mode: Write mode for saving to table ("overwrite" or "append")

        Returns:
            DataFrame: Final weekly zone analysis DataFrame
        """
        self.logger.info("Starting weekly zone analysis processing...")

        try:
            # Step 1: Calculate zone statistics with high fare analysis
            self.logger.info("Step 1: Calculating zone statistics with high fare analysis...")
            weekly_analysis = self.get_trips_with_fares_over_30(week_day)

            # Step 2: Validate data
            self.logger.info("Step 2: Validating weekly zone analysis data...")
            self.validate_weekly_analysis(weekly_analysis)

            # Step 3: Create target schema
            self.logger.info("Step 3: Creating target schema in Unity Catalog...")
            self.create_target_schema_in_catalog()

            # Step 4: Create target table
            self.logger.info("Step 4: Creating target table in Unity Catalog...")
            self.create_target_table_in_catalog()

            # Step 5: Save to catalog
            self.logger.info("Step 5: Saving weekly zone analysis to Unity Catalog...")
            self.save_to_catalog(weekly_analysis, mode)

            self.logger.info("Weekly zone analysis processing completed successfully!")

            # Log final statistics
            total_records = weekly_analysis.count()
            total_trips = weekly_analysis.agg(sum("trips_count")).collect()[0][0]
            total_high_fare = weekly_analysis.agg(sum("high_fare_count")).collect()[0][0]
            overall_high_fare_rate = (total_high_fare / total_trips * 100) if total_trips > 0 else 0

            self.logger.info("Final weekly zone analysis statistics:")
            self.logger.info(f"  Total zone-day records: {total_records:,}")
            self.logger.info(f"  Total trips analyzed: {total_trips:,}")
            self.logger.info(f"  High fare trips (>${self.HIGH_FARE_THRESHOLD}): {total_high_fare:,}")
            self.logger.info(f"  Overall high fare rate: {overall_high_fare_rate:.2f}%")
            self.logger.info(f"  Processing date: {self.current_date}")

            return weekly_analysis

        except Exception as e:
            self.logger.error(f"Weekly zone analysis processing failed: {e}")
            raise


def main():
    """Main execution function."""
    spark = SparkSession.builder \
        .appName("WeeklyZoneAnalysisProcessor") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        processor = WeeklyZoneAnalysisProcessor(spark)
        processor.process_weekly_zone_analysis()

        logger.info("Weekly zone analysis pipeline execution completed successfully!")

    except Exception as e:
        logger.error(f"Weekly zone analysis pipeline execution failed: {e}")
        raise
    finally:
        if not is_databricks_environment():
            try:
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")


if __name__ == "__main__":
    main()