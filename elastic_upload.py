from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import requests
import os
import dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


ENV = dotenv.dotenv_values(os.path.join(BASE_DIR, ".env"))
USERNAME = ENV.get("ES_USERNAME", "")
PASSWORD = ENV.get("ES_PASSWORD", "")
ES_URL = ENV.get("ES_URL", "")

def upload_pipelines(client: Elasticsearch, pipelines: dict):
    """
    client: Elasticsearch client
    pipelines: dict of pipeline names and their configurations
        pipelines={
            "master_velo_artifact_pipeline": {
                "description": "pipeline1 description",
                "processors": [
                    {
                        "set": {
                            "field": "field1",
                            "value": "value1"
                        }
                    }
                ]
            }
        }
    """

    for pipeline in pipelines:
        client.ingest.put_pipeline(id=pipeline, body=pipelines[pipeline])

def upload_dashboards(client: Elasticsearch, dashboards: dict):
    """
    client: Elasticsearch client
    dashboards: dict of dashboard names and their configurations
    """
    # for dashboard in dashboards:
    #     client.index(index=".kibana", body=dashboards[dashboard])
    for dashboard in dashboards:
        print("[*] Uploading dashboard:", dashboard.get("attributes").get("name"))

if __name__ == "__main__":
    
    from elastic_manager import setup_auth

    ES_URL = ENV.get("ES_URL", "")
    KIBANA_URI = ENV.get("KIBANA_URI", "")
    authenticated_es_client = setup_auth(ELASTIC_ENDPOINT=ES_URL)
    authenticated_kibana_client = setup_auth(ELASTIC_ENDPOINT=KIBANA_URI)