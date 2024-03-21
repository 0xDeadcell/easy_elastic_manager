from InquirerPy import prompt
from elasticsearch import Elasticsearch
from elastic_upload import upload_ndjson_objects, upload_multiple_pipelines, _get_pipeline_paths
from elastic_download import download_pipelines, download_dashboards, tabulate_dashboards, tabulate_pipelines
from elastic_migrate import migrate_pipelines, migrate_dashboards

from tabulate import tabulate
import dotenv
import json
import os
import time


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


ENV = dotenv.dotenv_values(os.path.join(BASE_DIR, ".env"))
USERNAME = ENV.get("ES_USERNAME", "")
PASSWORD = ENV.get("ES_PASSWORD", "")
ES_URL = ENV.get("ES_URL", "")


def create_directories(directory: str = BASE_DIR) -> bool: 
    os.makedirs(os.path.join(BASE_DIR, "stored_objects"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, "stored_objects", "dashboards"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, "stored_objects", "pipelines"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, "stored_objects", "pipelines", "master_pipeline"), exist_ok=True)
    os.makedirs(os.path.join(BASE_DIR, "stored_objects", "pipelines", "remap_pipelines"), exist_ok=True)
    print("[+] Created directories...")
    return True

def setup_auth(USERNAME: str,
               PASSWORD: str,
               KIBANA_URI: str,
               ELASTIC_ENDPOINT: str,
               ENCODED_API_KEY: str=None,
               api_key_name: str="ENCODED_API_KEY") -> Elasticsearch:
    # make a post request to the above endpoint to generate an API key
    if ENCODED_API_KEY:
        print("[*] API Key already exists, using existing key...")
    else:
        client = Elasticsearch(
            ELASTIC_ENDPOINT,
            basic_auth=(USERNAME, PASSWORD)
        )
        res = client.security.create_api_key(
            name="python-api-key"
        )

        # save api key to .env file, overwrite any existing keys
        ENCODED_API_KEY = res["encoded"]
        dotenv.set_key(os.path.join(BASE_DIR, ".env"), api_key_name, ENCODED_API_KEY)
        print("[+] API Key saved to .env file...")

    # create a new client with the generated API key
    client = Elasticsearch(
        ELASTIC_ENDPOINT,
        api_key=ENCODED_API_KEY
    )
    time.sleep(2)
    if client.ping():
        print("[+] Connected to Elasticsearch...")
        table = tabulate(
            [
                ["API Key", ENCODED_API_KEY],
                ["Username", USERNAME],
                ["Elastic Endpoint", ELASTIC_ENDPOINT],
                ["Kibana URI", KIBANA_URI]
            ],
            headers=["Key", "Value"],
            tablefmt="pretty"
        )
        print(table)
    else:
        print(f"[-] Could not connect to {ELASTIC_ENDPOINT}...")
        print("[!] Exiting...")
        exit(1)

    return client

def elastic_manager(source_client: Elasticsearch = None, target_client: Elasticsearch = None):
    if not target_client:
        try:
            dotenv.load_dotenv(os.path.join(BASE_DIR, ".env"))
            TARGET_ENCODED_API_KEY = ENV.get("ENCODED_API_KEY", "")
            TARGET_USERNAME = ENV.get("ES_USERNAME", "")
            TARGET_PASSWORD = ENV.get("ES_PASSWORD", "")
            TARGET_ES_URL = ENV.get("ES_URL", "")
            TARGET_KIBANA_URI = ENV.get("KIBANA_URI", "")
            target_client = setup_auth(
                                ELASTIC_ENDPOINT=TARGET_ES_URL,
                                USERNAME=TARGET_USERNAME,
                                PASSWORD=TARGET_PASSWORD,
                                ENCODED_API_KEY=TARGET_ENCODED_API_KEY,
                                KIBANA_URI=TARGET_KIBANA_URI,
                                api_key_name="ENCODED_API_KEY"

                                )
        except Exception as e:
            print(f"[-] {e}")
            print("[-] Could not connect to Elasticsearch... Please check your credentials in .env and try again.")
            print("[!] Exiting...")
            exit(1)
    if not source_client:
        try:
            dotenv.load_dotenv(os.path.join(BASE_DIR, ".env"))
            SOURCE_ENCODED_API_KEY = ENV.get("SOURCE_ENCODED_API_KEY", "")
            SOURCE_USERNAME = ENV.get("SOURCE_ES_USERNAME", "")
            SOURCE_PASSWORD = ENV.get("SOURCE_ES_PASSWORD", "")
            SOURCE_ES_URL = ENV.get("SOURCE_ES_URL", "")
            SOURCE_KIBANA_URI = ENV.get("SOURCE_KIBANA_URI", "")
            source_client = setup_auth(
                                ELASTIC_ENDPOINT=SOURCE_ES_URL,
                                USERNAME=SOURCE_USERNAME,
                                PASSWORD=SOURCE_PASSWORD,
                                ENCODED_API_KEY=SOURCE_ENCODED_API_KEY,
                                KIBANA_URI=SOURCE_KIBANA_URI,
                                api_key_name="SOURCE_ENCODED_API_KEY"
                                )
        except Exception as e:
            print(f"[-] {e}")
            print("[-] Could not connect to Elasticsearch... Please check your credentials in .env and try again.")
            print("[!] Exiting...")
            exit(1)

    
    questions = [
        {
            "type": "list",
            "message": "What would you like to do?",
            "name": "action",
            "choices": [
                f'Migrate Current Pipelines & Dashboards -> {TARGET_ES_URL}',
                "Download Pipelines & Dashboards",
                "Upload Pipelines & Dashboards",
                
                f'Migrate Only Pipelines -> {TARGET_ES_URL}',
                "Download Dashboards",
                "Download Pipelines",
                
                f'Migrate Only Dashboards -> {TARGET_ES_URL}',
                "Upload Dashboards",
                "Upload Pipelines",

                "Print Local Pipelines"
            ]
        }, 
        {"type": "confirm", "message": "Are you sure?", "name": "confirm", "default": True}
    ]
    answers = prompt(questions)
    action = answers["action"]
    confirm = answers["confirm"]
    # confirm = True
    if confirm:
        if action == "Download Pipelines":
            pipelines = download_pipelines(client=source_client)
            tabulate_pipelines(pipelines=pipelines)
        elif action == "Download Dashboards":
            dashboards = download_dashboards(KIBANA_URI=SOURCE_KIBANA_URI)
            tabulate_dashboards(dashboards=dashboards)

        elif action == "Download Pipelines & Dashboards":
            pipelines = download_pipelines(client=source_client)
            dashboards = download_dashboards(client=source_client)
            tabulate_dashboards(dashboards=dashboards)
            tabulate_pipelines(pipelines=pipelines)


        elif action == "Upload Pipelines":
            upload_multiple_pipelines(client=source_client)
        elif action == "Upload Dashboards":
            upload_ndjson_objects(KIBANA_URI=SOURCE_KIBANA_URI, USERNAME=SOURCE_USERNAME, PASSWORD=SOURCE_PASSWORD)
        elif action == "Upload Pipelines & Dashboards":
            upload_multiple_pipelines(client=source_client)
            upload_ndjson_objects(KIBANA_URI=SOURCE_KIBANA_URI, USERNAME=SOURCE_USERNAME, PASSWORD=SOURCE_PASSWORD)
        elif action == "Print Local Pipelines & Dashboards":
            local_pipelines = _get_pipeline_paths()
            for pipeline in local_pipelines:
                print(f"[*] {pipeline}")
            with open(os.path.join(BASE_DIR, "stored_objects", "dashboards", "dashboards.ndjson"), "rb") as f:
                dashboards = f.read()
                dashboards = [json.loads(dashboard) for dashboard in dashboards.decode("utf-8").splitlines() if dashboard.strip()]
                tabulate_dashboards(dashboards=dashboards)
            return elastic_manager(source_client=source_client, target_client=target_client)
        elif action == f'Migrate Current Pipelines & Dashboards -> {TARGET_ES_URL}':
            print("[*] Migrating pipelines...")
            migrate_pipelines(source_client=source_client, target_client=target_client)
            print("[*] Migrating dashboards...")
            migrate_dashboards(
                SOURCE_KIBANA_URI=SOURCE_KIBANA_URI,
                SOURCE_USERNAME=SOURCE_USERNAME,
                SOURCE_PASSWORD=SOURCE_PASSWORD,
                TARGET_KIBANA_URI=TARGET_KIBANA_URI,
                TARGET_USERNAME=TARGET_USERNAME,
                TARGET_PASSWORD=TARGET_PASSWORD
            )
        elif action == f"Migrate Only Pipelines -> {TARGET_ES_URL}":
            print("[*] Migrating pipelines...")
            migrate_pipelines(source_client=source_client, target_client=target_client)
        elif action == f"Migrate Only Dashboards -> {TARGET_ES_URL}":
            print("[*] Migrating dashboards...")
            migrate_dashboards(
                SOURCE_KIBANA_URI=SOURCE_KIBANA_URI,
                SOURCE_USERNAME=SOURCE_USERNAME,
                SOURCE_PASSWORD=SOURCE_PASSWORD,
                TARGET_KIBANA_URI=TARGET_KIBANA_URI,
                TARGET_USERNAME=TARGET_USERNAME,
                TARGET_PASSWORD=TARGET_PASSWORD
            )

        
    else:
        return elastic_manager(source_client=source_client, target_client=target_client)


if __name__ == "__main__":
    # print("What would you like to do?")
    create_directories()
    elastic_manager()