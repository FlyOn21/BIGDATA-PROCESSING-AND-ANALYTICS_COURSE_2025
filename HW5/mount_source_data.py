from typing import Dict, Any, Literal, Optional


def mount_source_data(extra_configs: Optional[Dict[str, Any]] = None, mount_name: str = "zhogolev-pv-temp-files", source: str = "s3a://zhogolev-pv-temp-files/"):
    """
    Mounts the source data from S3 to the Databricks file system.
    """
    try:
        if extra_configs is None:
            extra_configs = {}
        dbutils.fs.mount(
            source=source,
            mount_point=f"/mnt/{mount_name}",
            extra_configs=extra_configs
        )
        print("Mounting successful.")
        print(dbutils.fs.ls(f"/mnt/{mount_name}"))
    except Exception as e:
        print(f"Error mounting source data: {e}")


def unmount_source_data(mount_name: str = "zhogolev-pv-temp-files"):
    """
    Unmounts the source data from the Databricks file system.
    """
    try:
        dbutils.fs.unmount(f"/mnt/{mount_name}")
        print("Unmounting successful.")
    except Exception as e:
        print(f"Error unmounting source data: {e}")


def main(action: Literal["mount", "unmount"], mount_name: str = "zhogolev-pv-temp-files", source: str = "s3a://zhogolev-pv-temp-files/",
         extra_configs: Optional[Dict[str, Any]] = None):
    """
    Main function to mount or unmount the source data.
    """
    if action == "mount":
        mount_source_data(mount_name=mount_name, extra_configs=extra_configs, source=source)
    elif action == "unmount":
        unmount_source_data(mount_name)
    else:
        raise ValueError("Invalid action. Use 'mount' or 'unmount'.")


if __name__ == "__main__":
    main(action="mount", mount_name="zhogolev-pv-temp-files")
    print("Mounting source data completed.")
