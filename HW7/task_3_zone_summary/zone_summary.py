import logging
from datetime import UTC, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import DataFrame, avg, col, count, lit, max, min, row_number, sum
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from HW7.utils.check_is_databricks_env import is_databricks_environment


class ZoneSummaryProcessor:
	"""
	A class to process zone-level summary statistics from NYC taxi trip data.
	Reads from the trips_with_all_zones table and creates aggregated zone metrics.
	"""

	def __init__(self, spark: SparkSession):
		"""
		Initialize the ZoneSummaryProcessor.

		Args:
		    spark: SparkSession instance
		"""
		self.spark = spark
		self.logger = logging.getLogger(__name__)

		# Configuration
		self.CATALOG_BASE_BUCKET = 's3://zhoholiev-pavlo-databricks-s3/catalogs'
		self.current_date = datetime.now(UTC).date().isoformat()

		# Source table configuration
		self.SOURCE_CATALOG_NAME = 'pavlo_zhoholiev_nyc_catalog'
		self.SOURCE_SCHEMA_NAME = 'trips_schema'
		self.SOURCE_TABLE_NAME = 'trips_with_all_zones'
		self.source_table_path = f'{self.SOURCE_CATALOG_NAME}.{self.SOURCE_SCHEMA_NAME}.{self.SOURCE_TABLE_NAME}'

		# Target table configuration
		self.TARGET_CATALOG_NAME = 'pavlo_zhoholiev_nyc_catalog'
		self.TARGET_SCHEMA_NAME = 'analytics_schema'
		self.TARGET_TABLE_NAME = 'zone_summary'
		self.target_table_path = f'{self.TARGET_CATALOG_NAME}.{self.TARGET_SCHEMA_NAME}.{self.TARGET_TABLE_NAME}'

		# Zone summary schema
		self.zone_summary_schema = self._get_zone_summary_schema()

	def _get_zone_summary_schema(self) -> StructType:
		"""Get the schema for zone summary data."""
		return StructType(
			[
				StructField('pickup_zone', StringType(), True),
				StructField('total_trips', LongType(), True),
				StructField('avg_trip_distance', DoubleType(), True),
				StructField('avg_total_amount', DoubleType(), True),
				StructField('avg_tip_amount', DoubleType(), True),
				StructField('yellow_share', DoubleType(), True),
				StructField('green_share', DoubleType(), True),
				StructField('max_trip_distance', DoubleType(), True),
				StructField('min_tip_amount', DoubleType(), True),
				StructField('avg_trip_duration_minutes', DoubleType(), True),
				StructField('avg_passenger_count', DoubleType(), True),
				StructField('most_common_pickup_hour', IntegerType(), True),
				StructField('processing_date', StringType(), True),
			]
		)

	def load_source_data(self) -> DataFrame:
		"""
		Loads source data from a specified Spark table and verifies data integrity.

		This function retrieves the source table from the configured Spark catalog and schema, verifies
		if the table exists, and proceeds to read the table into a DataFrame. Data integrity is checked
		by examining the total record count and the count of records with valid pickup zones. If the
		table is missing, empty, or any other error occurs during this process, the error is logged,
		and an exception is raised.

		:raises Exception: If the source table does not exist, is empty, or an error occurs while
		    loading the data or verifying data integrity.
		:raises Exception: If the source data table records contain no valid pickup zones.

		:return: A DataFrame containing data from the source table.
		:rtype: DataFrame
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

	def filter_valid_zones(self, trips_df: DataFrame) -> DataFrame:
		"""
		Filter trips to include only those with valid pickup zones.
		"""
		self.logger.info('Filtering trips with valid pickup zones...')

		initial_count = trips_df.count()
		filtered_df = trips_df.filter(col('pickup_zone').isNotNull())
		final_count = filtered_df.count()

		removed_count = initial_count - final_count
		self.logger.info('Zone filtering results:')
		self.logger.info(f'  Valid zone trips: {final_count:,}')
		self.logger.info(f'  Removed trips: {removed_count:,}')
		self.logger.info(f'  Removal rate: {(removed_count / initial_count * 100):.2f}%')

		return filtered_df

	def calculate_basic_metrics(self, trips_df: DataFrame) -> DataFrame:
		"""
		Calculate basic aggregation metrics by pickup zone.
		"""
		self.logger.info('Calculating basic zone metrics...')

		zone_basic_metrics = trips_df.groupBy('pickup_zone').agg(
			count('*').alias('total_trips'),
			avg('trip_distance').alias('avg_trip_distance'),
			avg('total_amount').alias('avg_total_amount'),
			avg('tip_amount').alias('avg_tip_amount'),
			max('trip_distance').alias('max_trip_distance'),
			min('tip_amount').alias('min_tip_amount'),
			avg('trip_duration_minutes').alias('avg_trip_duration_minutes'),
			avg('passenger_count').alias('avg_passenger_count'),
		)

		zone_basic_metrics.cache()
		zone_count = zone_basic_metrics.count()
		self.logger.info(f'Basic metrics calculated for {zone_count} zones')

		return zone_basic_metrics

	def calculate_taxi_type_shares(self, trips_df: DataFrame, total_trips_df: DataFrame) -> DataFrame:
		"""
		Calculate taxi type shares (yellow vs green) by pickup zone.
		"""
		self.logger.info('Calculating taxi type shares...')

		taxi_type_counts = trips_df.groupBy('pickup_zone', 'taxi_type').agg(count('*').alias('type_count'))

		taxi_shares = (
			taxi_type_counts.groupBy('pickup_zone').pivot('taxi_type', ['yellow', 'green']).sum('type_count').fillna(0)
		)

		taxi_shares_with_total = taxi_shares.join(total_trips_df.select('pickup_zone', 'total_trips'), 'pickup_zone')

		taxi_shares_final = taxi_shares_with_total.select(
			'pickup_zone',
			(col('yellow') / col('total_trips')).alias('yellow_share'),
			(col('green') / col('total_trips')).alias('green_share'),
		)

		taxi_shares.unpersist()
		self.logger.info('Taxi type shares calculated successfully')

		return taxi_shares_final

	def calculate_pickup_hour_mode(self, trips_df: DataFrame) -> DataFrame:
		"""
		Calculate the most common pickup hour for each zone.
		"""
		self.logger.info('Calculating most common pickup hours...')

		# Count trips by zone and hour
		hour_counts = trips_df.groupBy('pickup_zone', 'pickup_hour').agg(count('*').alias('hour_count'))

		from pyspark.sql.window import Window

		window_spec = Window.partitionBy('pickup_zone').orderBy(col('hour_count').desc())

		most_common_hours = (
			hour_counts.withColumn('rank', row_number().over(window_spec))
			.filter(col('rank') == 1)
			.select('pickup_zone', col('pickup_hour').alias('most_common_pickup_hour'))
		)

		self.logger.info('Most common pickup hours calculated successfully')

		return most_common_hours

	def create_zone_summary(self, trips_df: DataFrame) -> DataFrame:
		"""
		Create complete zone summary with all metrics.
		"""
		self.logger.info('Creating comprehensive zone summary...')

		# Calculate basic metrics
		zone_basic_metrics = self.calculate_basic_metrics(trips_df)

		# Calculate taxi type shares
		taxi_shares = self.calculate_taxi_type_shares(trips_df, zone_basic_metrics)

		# Calculate most common pickup hours
		pickup_hours = self.calculate_pickup_hour_mode(trips_df)

		# Join all metrics together
		zone_summary = (
			zone_basic_metrics.join(taxi_shares, 'pickup_zone')
			.join(pickup_hours, 'pickup_zone')
			.select(
				'pickup_zone',
				'total_trips',
				'avg_trip_distance',
				'avg_total_amount',
				'avg_tip_amount',
				'yellow_share',
				'green_share',
				'max_trip_distance',
				'min_tip_amount',
				'avg_trip_duration_minutes',
				'avg_passenger_count',
				'most_common_pickup_hour',
			)
			.withColumn('processing_date', lit(self.current_date))
			.orderBy(col('total_trips').desc())
		)

		# Clean up cached data
		zone_basic_metrics.unpersist()

		self.logger.info('Zone summary created successfully')

		return zone_summary

	def validate_zone_summary(self, zone_summary_df: DataFrame) -> None:
		"""
		Validate the zone summary data quality.
		"""
		self.logger.info('Validating zone summary data...')

		# Basic statistics
		total_zones = zone_summary_df.count()
		self.logger.info(f'Total zones: {total_zones}')

		null_zones = zone_summary_df.filter(col('pickup_zone').isNull()).count()
		negative_trips = zone_summary_df.filter(col('total_trips') < 0).count()
		invalid_shares = zone_summary_df.filter(
			(col('yellow_share') + col('green_share') < 0.99) | (col('yellow_share') + col('green_share') > 1.01)
		).count()

		self.logger.info('Data quality check:')
		self.logger.info(f'  Null zones: {null_zones}')
		self.logger.info(f'  Negative trip counts: {negative_trips}')
		self.logger.info(f'  Invalid taxi type shares: {invalid_shares}')

		if null_zones > 0 or negative_trips > 0 or invalid_shares > 0:
			self.logger.warning('Data quality issues detected!')

		# Show schema and sample data
		self.logger.info('Zone summary schema:')
		zone_summary_df.printSchema()

		self.logger.info('Top 10 zones by total trips:')
		zone_summary_df.show(10, truncate=False)

		self.logger.info('Sample taxi type share validation:')
		zone_summary_df.select(
			'pickup_zone',
			'yellow_share',
			'green_share',
			(col('yellow_share') + col('green_share')).alias('total_share'),
		).show(5)

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
                total_trips BIGINT,
                avg_trip_distance DOUBLE,
                avg_total_amount DOUBLE,
                avg_tip_amount DOUBLE,
                yellow_share DOUBLE,
                green_share DOUBLE,
                max_trip_distance DOUBLE,
                min_tip_amount DOUBLE,
                avg_trip_duration_minutes DOUBLE,
                avg_passenger_count DOUBLE,
                most_common_pickup_hour INT,
                processing_date STRING
            )
            USING DELTA
            LOCATION '{self.CATALOG_BASE_BUCKET}/{self.TARGET_CATALOG_NAME}/{self.TARGET_SCHEMA_NAME}/{self.TARGET_TABLE_NAME}'
        """)

	def save_to_catalog(self, zone_summary_df: DataFrame, mode: str = 'overwrite') -> None:
		"""
		Saves the given DataFrame to a catalog table with the specified save mode.
		This method logs the saving process and verifies by reading back the data,
		counting the records, and displaying a preview of the saved data.

		:param zone_summary_df: The DataFrame containing the zone summary data to be saved.
		:param mode: The write mode to use for saving the data. Default is "overwrite".
		:return: None
		"""
		self.logger.info(f'Saving zone summary to catalog table: {self.target_table_path}')

		zone_summary_df.write.mode(mode).option('mergeSchema', 'true').saveAsTable(self.target_table_path)

		self.logger.info(f'Zone summary saved successfully to {self.target_table_path}')

		self.logger.info('Verifying saved data...')
		saved_zone_summary = self.spark.read.table(self.target_table_path)
		saved_count = saved_zone_summary.count()
		self.logger.info(f'Saved records count: {saved_count:,}')

		saved_zone_summary.show(5, truncate=False)

	def process_zone_summary(self, mode: str = 'overwrite') -> DataFrame:
		"""
		Main method to process zone summary data end-to-end.
		"""
		self.logger.info('Starting zone summary processing...')

		try:
			# Step 1: Load source data
			self.logger.info('Step 1: Loading source trip data...')
			trips_df = self.load_source_data()

			# Step 2: Filter for valid zones
			self.logger.info('Step 2: Filtering trips with valid pickup zones...')
			trips_with_valid_zones = self.filter_valid_zones(trips_df)

			# Step 3: Create zone summary
			self.logger.info('Step 3: Creating zone summary with all metrics...')
			zone_summary = self.create_zone_summary(trips_with_valid_zones)

			# Step 4: Validate data
			self.logger.info('Step 4: Validating zone summary data...')
			self.validate_zone_summary(zone_summary)

			# Step 5: Create target schema
			self.logger.info('Step 5: Creating target schema in Unity Catalog...')
			self.create_target_schema_in_catalog()

			# Step 6: Create target table
			self.logger.info('Step 6: Creating target table in Unity Catalog...')
			self.create_target_table_in_catalog()

			# Step 7: Save to catalog
			self.logger.info('Step 7: Saving zone summary to Unity Catalog...')
			self.save_to_catalog(zone_summary, mode)

			self.logger.info('Zone summary processing completed successfully!')

			# Log final statistics
			total_zones = zone_summary.count()
			total_trips = zone_summary.agg(sum('total_trips')).collect()[0][0]

			self.logger.info('Final zone summary statistics:')
			self.logger.info(f'  Total zones processed: {total_zones:,}')
			self.logger.info(f'  Total trips summarized: {total_trips:,}')
			self.logger.info(f'  Processing date: {self.current_date}')

			return zone_summary

		except Exception as e:
			self.logger.error(f'Zone summary processing failed: {e}')
			raise


def main():
	"""Main execution function."""
	spark = (
		SparkSession.builder.appName('ZoneSummaryProcessor')
		.config('spark.sql.adaptive.enabled', 'true')
		.config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
		.getOrCreate()
	)

	logging.basicConfig(level=logging.INFO)
	logger = logging.getLogger(__name__)

	try:
		processor = ZoneSummaryProcessor(spark)
		processor.process_zone_summary()

		logger.info('Zone summary pipeline execution completed successfully!')

	except Exception as e:
		logger.error(f'Zone summary pipeline execution failed: {e}')
		raise
	finally:
		if not is_databricks_environment():
			try:
				spark.stop()
				logger.info('Spark session stopped successfully')
			except Exception as e:
				logger.warning(f'Error stopping Spark session: {e}')


if __name__ == '__main__':
	main()
