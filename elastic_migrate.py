from elasticsearch import Elasticsearch
import json
import requests
import os
import dotenv
from tabulate import tabulate
from elastic_download import _get_pipeline_paths, _get_pipeline_name, _get_dashboard_paths, download_pipelines, download_dashboards
from elastic_upload import upload_pipeline, upload_multiple_pipelines, upload_ndjson_objects


def migrate_pipelines(source_client: Elasticsearch, target_client: Elasticsearch, pipeline_dir: str=os.path.join(os.path.dirname(os.path.abspath(__file__)), "stored_objects", "pipelines")) -> dict:
    # download pipelines from source client
    download_pipelines(client=source_client)
    # upload pipelines to target client
    return upload_multiple_pipelines(client=target_client, pipeline_dir=pipeline_dir)

def migrate_dashboards(
        SOURCE_KIBANA_URI: str,
        SOURCE_USERNAME: str,
        SOURCE_PASSWORD: str,
        TARGET_KIBANA_URI: str,
        TARGET_USERNAME: str,
        TARGET_PASSWORD: str,
        dashboard_dir: str=os.path.join(os.path.dirname(os.path.abspath(__file__)), "stored_objects", "dashboards")
                       ) -> dict:
    """
    SOURCE_KIBANA_URI: Kibana URI
    SOURCE_USERNAME: Kibana username
    SOURCE_PASSWORD: Kibana password
    TARGET_KIBANA_URI: Kibana URI
    TARGET_USERNAME: Kibana username
    TARGET_PASSWORD: Kibana password
    dashboard_dir: directory containing dashboard ndjson files
    
    Returns a dictionary of dashboard names and their upload status
    """
    # download dashboards from source kibana
    download_dashboards(KIBANA_URI=SOURCE_KIBANA_URI, USERNAME=SOURCE_USERNAME, PASSWORD=SOURCE_PASSWORD)
    # upload dashboards to target kibana
    return upload_ndjson_objects(
                                KIBANA_URI=TARGET_KIBANA_URI,
                                USERNAME=TARGET_USERNAME,
                                PASSWORD=TARGET_PASSWORD,
                                object_dir=dashboard_dir
                                )

if __name__ == "__main__":
    from elastic_manager import setup_auth
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ENV = dotenv.dotenv_values(os.path.join(BASE_DIR, ".env"))

    ES_URL=ENV.get("ES_URL", "")
    KIBANA_URI=ENV.get("KIBANA_URI", "")
    ES_USERNAME=ENV.get("ES_USERNAME", "")
    ES_PASSWORD=ENV.get("ES_PASSWORD", "")
    ENCODED_API_KEY=ENV.get("ENCODED_API_KEY", "")

    SOURCE_ES_URL=ENV.get("SOURCE_ES_URL", "")
    SOURCE_KIBANA_URI=ENV.get("SOURCE_KIBANA_URI", "")
    SOURCE_ES_USERNAME=ENV.get("SOURCE_ES_USERNAME", "")
    SOURCE_ES_PASSWORD=ENV.get("SOURCE_ES_PASSWORD", "")
    SOURCE_ENCODED_API_KEY=ENV.get("SOURCE_ENCODED_API_KEY", "")

    source_client = setup_auth(
        ELASTIC_ENDPOINT=SOURCE_ES_URL,
        USERNAME=SOURCE_ES_USERNAME,
        PASSWORD=SOURCE_ES_PASSWORD,
        ENCODED_API_KEY=SOURCE_ENCODED_API_KEY,
        KIBANA_URI=SOURCE_KIBANA_URI
    )

    target_client = setup_auth(
        ELASTIC_ENDPOINT=ES_URL,
        USERNAME=ES_USERNAME,
        PASSWORD=ES_PASSWORD,
        ENCODED_API_KEY=ENCODED_API_KEY,
        KIBANA_URI=KIBANA_URI
    )

    print("[*] Migrating pipelines...")
    migrated_pipelines = migrate_pipelines(source_client=source_client, target_client=target_client)
    print("[*] Migrated pipelines:")
    print(migrated_pipelines)
    print("[*] Migrating dashboards...")
    migrated_dashboards = migrate_dashboards(
        SOURCE_KIBANA_URI=SOURCE_KIBANA_URI,
        SOURCE_USERNAME=SOURCE_ES_USERNAME,
        SOURCE_PASSWORD=SOURCE_ES_PASSWORD,
        TARGET_KIBANA_URI=KIBANA_URI,
        TARGET_USERNAME=ES_USERNAME,
        TARGET_PASSWORD=ES_PASSWORD
    )
    print("[*] Migrated dashboards:")
    print(migrated_dashboards)