import logging
from datetime import UTC, datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from HW_7.utils.check_is_databricks_env import is_databricks_environment


class WeeklyZoneAnalysisProcessor:
    def __init__(self, spark: SparkSession):
        """
        Initializes the class with essential configurations and setups required
        for processing data. This includes setting up the Spark session, logger,
        and defining constants and mappings for data sources and targets.

        :param spark: An instance of SparkSession to facilitate operations.
        :type spark: SparkSession
        """
        self.spark = spark
        self.logger = logging.getLogger(__name__)

        # Configuration
        self.CATALOG_BASE_BUCKET = 's3://zhoholiev-pavlo-databricks-s3/catalogs'
        self.current_date = datetime.now(UTC).date().isoformat()

        # High fare threshold
        self.HIGH_FARE_THRESHOLD = 30.0

        # Day names mapping
        self.day_names = {
            1: 'Monday',
            2: 'Tuesday',
            3: 'Wednesday',
            4: 'Thursday',
            5: 'Friday',
            6: 'Saturday',
            7: 'Sunday',
        }

        # Source table configuration (output from TaxiDataProcessor)
        self.SOURCE_CATALOG_NAME = 'pavlo_zhoholiev_nyc_catalog'
        self.SOURCE_SCHEMA_NAME = 'trips_schema'
        self.SOURCE_TABLE_NAME = 'trips_with_all_zones'
        self.source_table_path = f'{self.SOURCE_CATALOG_NAME}.{self.SOURCE_SCHEMA_NAME}.{self.SOURCE_TABLE_NAME}'

        # Target table configuration
        self.TARGET_CATALOG_NAME = 'pavlo_zhoholiev_nyc_catalog'
        self.TARGET_SCHEMA_NAME = 'analytics_schema'
        self.TARGET_TABLE_NAME = 'trips_with_fares_over_30'
        self.target_table_path = f'{self.TARGET_CATALOG_NAME}.{self.TARGET_SCHEMA_NAME}.{self.TARGET_TABLE_NAME}'

        # Weekly zone analysis schema
        self.weekly_zone_schema = self._get_weekly_zone_schema()

    def _get_weekly_zone_schema(self) -> StructType:
        """Get the schema for weekly zone analysis data."""
        return StructType(
            [
                StructField('pickup_zone', StringType(), True),
                StructField('day_of_week', IntegerType(), True),
                StructField('day_name', StringType(), True),
                StructField('trips_count', LongType(), True),
                StructField('high_fare_share', DoubleType(), True),
                StructField('high_fare_count', LongType(), True),
                StructField('avg_total_amount', DoubleType(), True),
                StructField('avg_trip_distance', DoubleType(), True),
                StructField('max_fare', DoubleType(), True),
                StructField('min_fare', DoubleType(), True),
                StructField('processing_date', StringType(), True),
            ]
        )

    def load_source_data(self) -> DataFrame:
        """
        Loads source data from a specified table, verifies its existence, ensures data
        integrity, and logs detailed information about the source data. This method
        uses Spark to interact with the source table and perform data operations.

        :rtype: pyspark.sql.DataFrame

        :return: A Spark DataFrame containing source data.
        :raises Exception: If the source table does not exist, is empty, or there are issues
                           loading the data.
        """
        try:
            self.logger.info(f'Loading source data from: {self.source_table_path}')

            # Check if source table exists
            tables = self.spark.sql(f'SHOW TABLES IN {self.SOURCE_CATALOG_NAME}.{self.SOURCE_SCHEMA_NAME}').collect()
            table_names = [row.tableName for row in tables]

            if self.SOURCE_TABLE_NAME not in table_names:
                raise Exception(
                    f'Source table {self.source_table_path} does not exist. Please run TaxiDataProcessor first.'
                )

            df = self.spark.read.table(self.source_table_path)

            # Verify data integrity
            total_count = df.count()
            valid_zones_count = df.filter(col('pickup_zone').isNotNull()).count()

            self.logger.info('Source data loaded successfully:')
            self.logger.info(f'  Total records: {total_count:,}')
            self.logger.info(f'  Records with valid pickup zones: {valid_zones_count:,}')
            self.logger.info(f'  Columns: {len(df.columns)}')

            if total_count == 0:
                raise Exception('Source table is empty')

            return df

        except Exception as e:
            self.logger.error(f'Failed to load source data: {e}')
            raise

    def get_trips_with_fares_over_30(self, week_day: int | None = None) -> DataFrame:
        """
        Analyze and process trip data to find trips with fares exceeding a specified
        high fare threshold for specified days of the week using optimized single-pass aggregation.

        :param week_day: Optional. An integer representing the day of the week (1 for Monday,
            7 for Sunday). If not provided, all days of the week will be processed.
        :type week_day: Optional[int]
        :raises ValueError: If `week_day` is provided but is not within the range 1 to 7.
        :return: A dataset containing statistics for zones and days where the fare
            exceeded the defined threshold.
        :rtype: PySpark DataFrame
        """
        self.logger.info(f'Starting high fare analysis with threshold: ${self.HIGH_FARE_THRESHOLD}')

        # Validate week_day parameter
        if week_day is not None and week_day not in self.day_names:
            raise ValueError(f'week_day argument must be between 1-7, got {week_day}')

        # Load and filter source data
        df = (
            self.load_source_data()
            .filter(col('pickup_zone').isNotNull())
        )

        # Filter by specific day if requested
        if week_day is not None:
            df = df.filter(col('pickup_day_of_week') == week_day)
            self.logger.info(f'Processing single day: {self.day_names[week_day]}')
        else:
            self.logger.info('Processing all days of the week')

        # Create day name mapping for CASE expression
        case_expr = "CASE pickup_day_of_week " + \
                    " ".join([f"WHEN {k} THEN '{v}'" for k, v in self.day_names.items()]) + \
                    " END"

        # Single-pass aggregation by day and zone
        self.logger.info('Calculating zone statistics with optimized single-pass aggregation...')

        stats = (
            df.groupBy('pickup_day_of_week', 'pickup_zone')
            .agg(
                count('*').alias('trips_count'),
                sum(when(col('total_amount') > self.HIGH_FARE_THRESHOLD, 1).otherwise(0))
                .alias('high_fare_count'),
                avg('total_amount').alias('avg_total_amount'),
                avg('trip_distance').alias('avg_trip_distance'),
                max('total_amount').alias('max_fare'),
                min('total_amount').alias('min_fare'),
            )
            .withColumn('high_fare_share', col('high_fare_count') / col('trips_count'))
            .withColumn('day_name', expr(case_expr))
            .withColumn('processing_date', lit(self.current_date))
            .select(
                'pickup_zone',
                col('pickup_day_of_week').alias('day_of_week'),
                'day_name',
                'trips_count',
                'high_fare_share',
                'high_fare_count',
                'avg_total_amount',
                'avg_trip_distance',
                'max_fare',
                'min_fare',
                'processing_date',
            )
            .orderBy(desc('trips_count'))
        )

        # Single aggregation for comprehensive logging
        summary_stats = stats.agg(
            count('*').alias('total_zone_day_combinations'),
            sum('trips_count').alias('total_trips'),
            sum('high_fare_count').alias('total_high_fare_trips'),
            countDistinct('pickup_zone').alias('unique_zones'),
            countDistinct('day_of_week').alias('days_processed')
        ).collect()[0]

        # Log comprehensive statistics
        total_records = summary_stats['total_zone_day_combinations']
        total_trips = summary_stats['total_trips']
        total_high_fare = summary_stats['total_high_fare_trips']
        unique_zones = summary_stats['unique_zones']
        days_processed = summary_stats['days_processed']

        overall_high_fare_rate = (total_high_fare / total_trips * 100) if total_trips > 0 else 0

        self.logger.info('Processing completed successfully:')
        self.logger.info(f'  Zone-day combinations: {total_records:,}')
        self.logger.info(f'  Unique zones: {unique_zones:,}')
        self.logger.info(f'  Days processed: {days_processed}')
        self.logger.info(f'  Total trips: {total_trips:,}')
        self.logger.info(f'  High fare trips (>${self.HIGH_FARE_THRESHOLD}): {total_high_fare:,}')
        self.logger.info(f'  Overall high fare rate: {overall_high_fare_rate:.2f}%')

        return stats

    def validate_weekly_analysis(self, weekly_df: DataFrame) -> None:
        """
        Validate the weekly zone analysis data with optimized single-pass validation.
        """
        self.logger.info('Validating weekly zone analysis data...')

        # Single-pass validation with comprehensive aggregations
        validation_stats = weekly_df.agg(
            count('*').alias('total_records'),
            countDistinct('pickup_zone').alias('unique_zones'),
            countDistinct('day_of_week').alias('days_processed'),
            sum(when(col('pickup_zone').isNull(), 1).otherwise(0)).alias('null_zones'),
            sum(when(col('trips_count') < 0, 1).otherwise(0)).alias('negative_trips'),
            sum(when((col('high_fare_share') < 0) | (col('high_fare_share') > 1), 1).otherwise(0)).alias(
                'invalid_shares')
        ).collect()[0]

        # Extract validation results
        total_records = validation_stats['total_records']
        unique_zones = validation_stats['unique_zones']
        days_processed = validation_stats['days_processed']
        null_zones = validation_stats['null_zones']
        negative_trips = validation_stats['negative_trips']
        invalid_shares = validation_stats['invalid_shares']

        self.logger.info('Validation results:')
        self.logger.info(f'  Total zone-day combinations: {total_records:,}')
        self.logger.info(f'  Unique zones: {unique_zones:,}')
        self.logger.info(f'  Days processed: {days_processed}')

        self.logger.info('Data quality check:')
        self.logger.info(f'  Null zones: {null_zones}')
        self.logger.info(f'  Negative trip counts: {negative_trips}')
        self.logger.info(f'  Invalid high fare shares: {invalid_shares}')

        if null_zones > 0 or negative_trips > 0 or invalid_shares > 0:
            self.logger.warning('Data quality issues detected!')

        # Show schema and sample data
        self.logger.info('Weekly analysis schema:')
        weekly_df.printSchema()

        self.logger.info('Sample data for each day:')
        for day_num in self.day_names:
            day_data = weekly_df.filter(col('day_of_week') == day_num)
            day_count = day_data.count()
            if day_count > 0:
                self.logger.info(f'\n{self.day_names[day_num]} - Top 5 zones by trip count:')
                day_data.show(5, truncate=False)

    def create_target_schema_in_catalog(self):
        """Create target schema in Unity Catalog if it doesn't exist."""
        self.logger.info(f'Creating schema {self.TARGET_CATALOG_NAME}.{self.TARGET_SCHEMA_NAME} if it does not exist.')
        self.spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {self.TARGET_CATALOG_NAME}.{self.TARGET_SCHEMA_NAME}
        """)

    def create_target_table_in_catalog(self):
        """Create target table in Unity Catalog if it doesn't exist."""
        self.logger.info(f'Creating table {self.target_table_path} if it does not exist.')

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

    def save_to_catalog(self, weekly_df: DataFrame, mode: str = 'overwrite') -> None:
        """
        Saves the weekly zone analysis data to a specified catalog table, with options to merge schemas
        and partition data by processing date and day of the week. After saving, it verifies the data by
        counting the records and logging sample data for each day of the week.

        :param weekly_df: DataFrame containing the weekly zone analysis data to be saved
        :param mode: Writing mode for saving the data to the catalog table. Defaults to "overwrite".
        :return: None
        """
        self.logger.info(f'Saving weekly zone analysis to catalog table: {self.target_table_path}')

        weekly_df.write.mode(mode).option('mergeSchema', 'true').partitionBy(
            'processing_date', 'day_of_week'
        ).saveAsTable(self.target_table_path)

        self.logger.info(f'Weekly zone analysis saved successfully to {self.target_table_path}')

        self.logger.info('Verifying saved data...')
        saved_weekly_df = self.spark.read.table(self.target_table_path)
        saved_count = saved_weekly_df.count()
        self.logger.info(f'Saved records count: {saved_count:,}')

        self.logger.info('Sample of saved data by day:')
        for day_num in self.day_names:
            day_data = saved_weekly_df.filter(col('day_of_week') == day_num)
            if day_data.count() > 0:
                self.logger.info(f'\n{self.day_names[day_num]} - Top 3 zones:')
                day_data.show(3, truncate=False)

    def process_weekly_zone_analysis(self, week_day: int | None = None, mode: str = 'overwrite') -> DataFrame:
        """
        Main method to process weekly zone analysis data end-to-end with optimized performance.
        """
        self.logger.info('Starting weekly zone analysis processing...')

        try:
            # Step 1: Calculate zone statistics with high fare analysis (optimized)
            self.logger.info('Step 1: Calculating zone statistics with high fare analysis...')
            weekly_analysis = self.get_trips_with_fares_over_30(week_day)

            # Step 2: Validate data
            self.logger.info('Step 2: Validating weekly zone analysis data...')
            self.validate_weekly_analysis(weekly_analysis)

            # Step 3: Create target schema
            self.logger.info('Step 3: Creating target schema in Unity Catalog...')
            self.create_target_schema_in_catalog()

            # Step 4: Create target table
            self.logger.info('Step 4: Creating target table in Unity Catalog...')
            self.create_target_table_in_catalog()

            # Step 5: Save to catalog
            self.logger.info('Step 5: Saving weekly zone analysis to Unity Catalog...')
            self.save_to_catalog(weekly_analysis, mode)

            self.logger.info('Weekly zone analysis processing completed successfully!')

            # Final summary (already calculated in get_trips_with_fares_over_30)
            self.logger.info(f'Processing date: {self.current_date}')

            return weekly_analysis

        except Exception as e:
            self.logger.error(f'Weekly zone analysis processing failed: {e}')
            raise


def main():
    spark = (
        SparkSession.builder.appName('WeeklyZoneAnalysisProcessor')
        .config('spark.sql.adaptive.enabled', 'true')
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .getOrCreate()
    )

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        processor = WeeklyZoneAnalysisProcessor(spark)
        processor.process_weekly_zone_analysis()

        logger.info('Weekly zone analysis pipeline execution completed successfully!')

    except Exception as e:
        logger.error(f'Weekly zone analysis pipeline execution failed: {e}')
        raise
    finally:
        if not is_databricks_environment():
            try:
                spark.stop()
                logger.info('Spark session stopped successfully')
            except Exception as e:
                logger.warning(f'Error stopping Spark session: {e}')
