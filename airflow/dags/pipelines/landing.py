"""Landing zone: where raw files sit before the warehouse loads them.

Paths are relative ("exchange_rates/2026-09-10.json") and identical in both
modes, so the load step and the warehouse's FILE_NAME column don't care
whether the file lives on local disk or in Azure Blob Storage.
"""
import json
import os
import shutil

from pipelines.config import BLOB_CONTAINER, LANDING_DIR, PIPELINE_MODE, WASB_CONN_ID


def _wasb():
    # Imported lazily so local mode doesn't need the Azure provider
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
    return WasbHook(wasb_conn_id=WASB_CONN_ID)


def local_path(blob_name):
    return os.path.join(LANDING_DIR, blob_name)


def land_json(data, blob_name):
    if PIPELINE_MODE == "cloud":
        _wasb().load_string(
            string_data=json.dumps(data),
            container_name=BLOB_CONTAINER,
            blob_name=blob_name,
            overwrite=True,
        )
    else:
        path = local_path(blob_name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)
    print(f"Landed {blob_name}")
    return blob_name


def land_file(file_path, blob_name):
    if PIPELINE_MODE == "cloud":
        _wasb().load_file(
            file_path=file_path,
            container_name=BLOB_CONTAINER,
            blob_name=blob_name,
            overwrite=True,
        )
    else:
        path = local_path(blob_name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.abspath(file_path) != os.path.abspath(path):
            shutil.copyfile(file_path, path)
    print(f"Landed {blob_name}")
    return blob_name
