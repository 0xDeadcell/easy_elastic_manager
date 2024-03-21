from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import json
import requests
import os
import dotenv


BASE_DIR = os.path.dirname(os.path.abspath(__file__))



def upload_multiple_pipelines(client: Elasticsearch, pipeline_dir: str=os.path.join(BASE_DIR, "stored_objects", "pipelines")) -> dict:
    """
    client: Elasticsearch client
    pipeline_dir: directory containing pipeline json files
    
    Returns a dictionary of pipeline names and their upload status
    """

    valid_uploads = {}
    for pipeline_path in _get_pipeline_paths(pipeline_dir):
        try:
            with open(pipeline_path, "r") as f:
                pipeline = json.load(f) # load pipeline json file as a dictionary
        except Exception as e:
            print(e)
            print(f"[*] Could not load pipeline from file: {pipeline_path}")
            valid_uploads[pipeline_path] = False
            continue
        try:
            pipeline_name = _get_pipeline_name(pipeline)
            pipeline_data = pipeline[pipeline_name]
            result = upload_pipeline(client=client, pipeline_name=pipeline_name, pipeline_data=pipeline_data)
            valid_uploads[pipeline_name] = result
        except IndexError as e:
            print(e)
            print(f"[*] No pipeline found in:\n {pipeline}")
            valid_uploads[pipeline] = False
            # print(f"[*] Pipeline data: {pipeline_data}")
    return valid_uploads

def upload_pipeline(client: Elasticsearch, pipeline_name: str, pipeline_data: dict) -> bool:
    """
    client: Elasticsearch client
    pipeline_name: name of the pipeline
    pipeline_data: dictionary containing the pipeline configuration
    """
    assert isinstance(pipeline_data, dict), "pipeline_data must be a dictionary"
    client.ingest.put_pipeline(id=pipeline_name, body=pipeline_data)

    result = client.ingest.put_pipeline(id=pipeline_name, body=pipeline_data)
    print(f"[*] Uploaded pipeline: {pipeline_name}")
    # return result.ok
    return result.get("acknowledged", False)

def _get_pipeline_paths(pipeline_dir: str=os.path.join(BASE_DIR, "stored_objects", "pipelines")) -> list:
    abs_pipeline_paths = []
    for root, dirs, files in os.walk(pipeline_dir):
        for matching_file in files:
            if matching_file.endswith(".json"):
                abs_pipeline_paths.append(os.path.abspath(os.path.join(root, matching_file)))
    return abs_pipeline_paths

def _get_pipeline_name(pipeline: dict) -> str:
    try:
        assert isinstance(pipeline, dict), "pipeline must be a dictionary"
        pipeline_names = list(pipeline.keys()).pop(0)
        return pipeline_names
    except IndexError as e:
        print(e)
        print(f"[*] No pipeline found in:\n {pipeline}")
        return False

def _get_ndjson_object_paths(object_dir: str=os.path.join(BASE_DIR, "stored_objects", "dashboards")) -> list:
    abs_dashboard_paths = []
    for root, dirs, files in os.walk(object_dir):
        for matching_file in files:
            if matching_file.endswith(".ndjson"):
                abs_dashboard_paths.append(os.path.abspath(os.path.join(root, matching_file)))
    return abs_dashboard_paths

def upload_ndjson_objects(KIBANA_URI: str, USERNAME: str, PASSWORD: str, object_dir: str=os.path.join(BASE_DIR, "stored_objects", "dashboards")) -> dict:
    """
    KIBANA_URI: Kibana URI
    USERNAME: Kibana username
    PASSWORD: Kibana password
    object_dir: directory containing ndjson files
    
    Returns a dictionary of object paths and their upload success status

    curl -X POST "localhost:5601/api/saved_objects/_import?createNewCopies=true -H "kbn-xsrf: true" --form file=@file.ndjson" -H 'kbn-xsrf: true'

    """

    valid_uploads = {}
    # check if object_dir is a directory or a file
    f_name = os.path.basename(object_dir)
    for object_path in _get_ndjson_object_paths(object_dir):
        print("[*] Uploading dashboards & other objects...")

        headers = {
            # "Content-Type": "application/json",
            "kbn-xsrf": "true"
        }
        with open(object_path, "rb") as data:
            files = {
                "file": (os.path.basename(object_path), data, 'application/ndjson')
            }

            response = requests.post(
                f"{KIBANA_URI}/api/saved_objects/_import?createNewCopies=true",
                headers=headers,
                auth=(USERNAME, PASSWORD),
                files=files,
                timeout=1000,
                verify=True
            )
            try:
                res = json.loads(response.text)
                success = res.get("success", False)
                success_count = res.get("successCount", 0)
                valid_uploads[f_name] = {
                    "success": success,
                    "success_count": success_count
                }
            except Exception as e:
                print(e)
                print(f"[-] Failed to upload object: {object_path}")
                continue
    if valid_uploads[f_name].get("success", False):
        print(f"[*] Successfully Uploaded object: {object_path}")
        print(f"[*] Success count: {success_count}")
    elif valid_uploads[f_name].get("success", False) == False and valid_uploads[f_name].get("success_count", 0) == 0:
        print(f"[-] Failed to upload object completely: {object_path}")
        print(f"[-] Response: {res}")
    elif valid_uploads[f_name].get("success_count", 0) > 0:
        print(f"[*] Uploaded {success_count} object(s) from {object_path}")
    return valid_uploads


if __name__ == "__main__":
    ENV = dotenv.dotenv_values(os.path.join(BASE_DIR, ".env"))
    USERNAME = ENV.get("ES_USERNAME", "")
    PASSWORD = ENV.get("ES_PASSWORD", "")
    ES_URL = ENV.get("ES_URL", "")

    from elastic_manager import setup_auth

    ES_URL = ENV.get("ES_URL", "")
    KIBANA_URI = ENV.get("KIBANA_URI", "")
    authenticated_es_client = setup_auth(ELASTIC_ENDPOINT=ES_URL)
    authenticated_kibana_client = setup_auth(ELASTIC_ENDPOINT=KIBANA_URI)