from collections.abc import Sequence
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import *


class SchemaToSQLConverter:
    """
    Convert Spark DataFrame schemas to SQL CREATE TABLE statements.

    This class is designed to facilitate the conversion of Spark DataFrame schemas
    into SQL CREATE TABLE statements by mapping Spark data types to their SQL
    counterparts. It supports handling complex data types like Struct, Array, and
    Map, and allows customization through various parameters such as catalog,
    schema, and table format.

    :ivar catalog_base_bucket: Base location for catalog data.
    :type catalog_base_bucket: Optional[str]
    :ivar default_catalog: Default catalog to use if none is provided.
    :type default_catalog: Optional[str]
    :ivar default_schema: Default schema to use if none is provided.
    :type default_schema: Optional[str]
    :ivar table_format: Format of the table (e.g., DELTA, PARQUET).
    :type table_format: str
    """

    def __init__(
            self,
            catalog_base_bucket: str | None = None,
            default_catalog: str | None = None,
            default_schema: str | None = None,
            table_format: str = 'DELTA',
    ) -> None:
        """
        Initialize the converter with default settings
        """
        self.catalog_base_bucket = catalog_base_bucket
        self.default_catalog = default_catalog
        self.default_schema = default_schema
        self.table_format = table_format

        self.type_mapping = {
            StringType: 'STRING',
            IntegerType: 'INT',
            LongType: 'BIGINT',
            DoubleType: 'DOUBLE',
            FloatType: 'FLOAT',
            BooleanType: 'BOOLEAN',
            DateType: 'DATE',
            TimestampType: 'TIMESTAMP',
            BinaryType: 'BINARY',
            ByteType: 'TINYINT',
            ShortType: 'SMALLINT',
        }

    def __get_sql_type(self, spark_type: DataType) -> str:
        """Convert Spark data type to SQL type string"""
        spark_type_class = type(spark_type)

        if spark_type_class in self.type_mapping:
            return self.type_mapping[spark_type_class]

        if isinstance(spark_type, DecimalType):
            return f'DECIMAL({spark_type.precision},{spark_type.scale})'
        elif isinstance(spark_type, ArrayType):
            element_type = self.__get_sql_type(spark_type.elementType)
            return f'ARRAY<{element_type}>'
        elif isinstance(spark_type, MapType):
            key_type = self.__get_sql_type(spark_type.keyType)
            value_type = self.__get_sql_type(spark_type.valueType)
            return f'MAP<{key_type},{value_type}>'
        elif isinstance(spark_type, StructType):
            field_defs = []
            for field in spark_type.fields:
                field_type = self.__get_sql_type(field.dataType)
                field_defs.append(f'{field.name}:{field_type}')
            return f'STRUCT<{",".join(field_defs)}>'
        else:
            type_str = str(spark_type).upper()
            if 'TYPE' in type_str:
                return type_str.replace('TYPE', '')
            return type_str

    def convert_schema_to_sql(
            self,
            df: DataFrame,
            table_path: str,
            catalog_name: str | None = None,
            schema_name: str | None = None,
            table_name: str | None = None,
            partition_columns: Sequence[str] | None = None,
            location_override: str | None = None,
            table_format: str | None = None,
    ) -> str:
        """
        Generates a SQL `CREATE TABLE` statement based on the schema of a given DataFrame and
        additional options such as partitioning, location, and table format.

        :param df: The DataFrame whose schema will be used to generate the SQL statement.
        :param table_path: Fully qualified name of the table in the format `<catalog>.<schema>.<table>`.
        :param catalog_name: Optional name of the catalog to use. If not specified, defaults to
            the object's default catalog.
        :param schema_name: Optional name of the schema to use. If not specified, defaults to
            the object's default schema.
        :param table_name: Optional name of the table. If provided, used to calculate the default
            location if `location_override` is not specified.
        :param partition_columns: Optional list of columns to be used for partitioning the table.
        :param location_override: Optional explicit location for where the table data resides.
        :param table_format: Optional format for the table (e.g., PARQUET, DELTA). If not
            specified, defaults to the object's table format.
        :return: A string containing the SQL `CREATE TABLE` statement.
        """
        catalog_name = catalog_name or self.default_catalog
        schema_name = schema_name or self.default_schema
        table_format = table_format or self.table_format

        schema = df.schema
        df.printSchema()
        column_definitions = []

        for field in schema.fields:
            col_name = field.name
            sql_type = self.__get_sql_type(field.dataType)
            column_definitions.append(f'        {col_name} {sql_type}')

        # Build the CREATE TABLE statement
        columns_sql = ',\n'.join(column_definitions)

        # Handle partitioning
        partition_clause = ''
        if partition_columns:
            partition_clause = f'\n    PARTITIONED BY ({", ".join(partition_columns)})'

        # Handle location
        if location_override:
            location = location_override
        elif self.catalog_base_bucket and catalog_name and schema_name and table_name:
            location = f'{self.catalog_base_bucket}/{catalog_name}/{schema_name}/{table_name}'
        else:
            location = None

        location_clause = f"\n    LOCATION '{location}'" if location else ''

        return f"""CREATE TABLE IF NOT EXISTS {table_path} ({columns_sql})
    USING {table_format}{partition_clause}{location_clause}"""
