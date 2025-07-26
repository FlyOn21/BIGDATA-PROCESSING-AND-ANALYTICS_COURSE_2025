def mount_source_data(extra_configs=None):
    """
    Mounts the source data from S3 to the Databricks file system.
    """
    try:
        if extra_configs is None:
            extra_configs = {}
        dbutils.fs.mount(
            source="s3a://zhogolev-pv-temp-files/",
            mount_point="/mnt/zhogolev-pv-temp-files5",
            extra_configs=extra_configs
        )
        print("Mounting successful.")
    except Exception as e:
        print(f"Error mounting source data: {e}")

if __name__ == "__main__":
    mount_source_data()
    print("Mounting source data completed.")