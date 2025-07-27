def is_databricks_environment():
	"""
	Determine if the current environment is a Databricks environment.

	This function checks for the availability of the `pyspark.dbutils` module
	to determine whether the code is running in a Databricks environment.
	If the module import succeeds, it returns True indicating the Databricks
	environment; otherwise, it returns False.

	:return: A boolean indicating whether the current environment is
	         a Databricks environment (True) or not (False)
	:rtype: bool
	"""
	try:
		import pyspark.dbutils

		return True
	except ImportError:
		return False
