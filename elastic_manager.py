from InquirerPy import prompt
from elasticsearch import Elasticsearch
from elastic_upload import upload_pipelines, upload_dashboards
from elastic_download import download_pipelines, download_dashboards, tabulate_dashboards, tabulate_pipelines

from tabulate import tabulate
import dotenv
import json
import os


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

def setup_auth(ELASTIC_ENDPOINT: str = ES_URL) -> Elasticsearch:
    dotenv.load_dotenv(os.path.join(BASE_DIR, ".env"))
    ENCODED_API_KEY = ENV.get("ENCODED_API_KEY", "")
    USERNAME = ENV.get("ES_USERNAME", "")
    PASSWORD = ENV.get("ES_PASSWORD", "")

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
        dotenv.set_key(os.path.join(BASE_DIR, ".env"), "ENCODED_API_KEY", ENCODED_API_KEY)
        print("[+] API Key saved to .env file...")

    # create a new client with the generated API key
    client = Elasticsearch(
        ELASTIC_ENDPOINT,
        api_key=ENCODED_API_KEY
    )
    if client.ping():
        print("[+] Connected to Elasticsearch...")
        table = tabulate(
            [
                ["API Key", ENCODED_API_KEY],
                ["Username", USERNAME],
                ["Elastic Endpoint", ELASTIC_ENDPOINT],
                ["Kibana URI", ENV.get("KIBANA_URI", "")]
            ],
            headers=["Key", "Value"],
            tablefmt="pretty"
        )
        print(table)

    return client

def elastic_manager(client: Elasticsearch = None):
    if not client:
        try:
            client = setup_auth(ELASTIC_ENDPOINT=ES_URL)
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
                "Download Pipelines & Dashboards",
                "Upload Pipelines & Dashboards",

                "Download Dashboards",
                "Download Pipelines",
                
                "Upload Dashboards",
                "Upload Pipelines"
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
            pipelines = download_pipelines(client=client)
            tabulate_pipelines(pipelines=pipelines)
        elif action == "Download Dashboards":
            dashboards = download_dashboards(client=client)
            tabulate_dashboards(dashboards=dashboards)

        elif action == "Download Pipelines & Dashboards":
            pipelines = download_pipelines(client=client)
            dashboards = download_dashboards(client=client)
            tabulate_dashboards(dashboards=dashboards)
            tabulate_pipelines(pipelines=pipelines)


        elif action == "Upload Pipelines":
            upload_pipelines(client=client)
        elif action == "Upload Dashboards":
            upload_dashboards(client=client)
        elif action == "Upload Pipelines & Dashboards":
            upload_pipelines(client=client)
            upload_dashboards(client=client)
    else:
        return elastic_manager(client=client)


if __name__ == "__main__":
    # print("What would you like to do?")
    create_directories()
    elastic_manager()