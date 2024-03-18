from elasticsearch import Elasticsearch
import json
import requests
import os
import dotenv
from tabulate import tabulate

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


ENV = dotenv.dotenv_values(os.path.join(BASE_DIR, ".env"))
USERNAME = ENV.get("ES_USERNAME", "")
PASSWORD = ENV.get("ES_PASSWORD", "")
ES_URL = ENV.get("ES_URL", "")

# Download Pipelines
def download_pipelines(client: Elasticsearch):
    remap_pipelines = client.ingest.get_pipeline(id=".REMAP*")
    master_pipeline = client.ingest.get_pipeline(id="master*")
    if not os.path.exists(os.path.join(BASE_DIR, "stored_objects", "pipelines")):
        print("[*] Creating directories...")
        os.makedirs(os.path.join(BASE_DIR, "stored_objects", "pipelines", "master_pipeline"), exist_ok=True)
        os.makedirs(os.path.join(BASE_DIR, "stored_objects", "pipelines", "remap_pipelines"), exist_ok=True)
    print("[*] Downloading pipelines...")
    pipelines = {}
    for pipeline in remap_pipelines:
        pipelines[pipeline] = remap_pipelines[pipeline]
        with open(os.path.join(BASE_DIR, "stored_objects", "pipelines", "remap_pipelines", f"{pipeline}.json"), "w") as f:
            f.write(json.dumps({pipeline: pipelines[pipeline]}, indent=4))

    for pipeline in master_pipeline:
        pipelines[pipeline] = master_pipeline[pipeline]
        with open(os.path.join(BASE_DIR, "stored_objects", "pipelines", "master_pipeline", f"{pipeline}.json"), "w") as f:
            f.write(json.dumps({pipeline: pipelines[pipeline]}, indent=4))
    print("[+] Pipelines downloaded successfully...")
    return pipelines

def tabulate_pipelines(pipelines: dict):
    """
    pipelines: dict of pipeline names and their configurations
    """
    # order the pipelines by name, alphabetically
    pipelines = dict(sorted(pipelines.items(), key=lambda x: x[0]))
    reformatted_pipelines = []
    for pipeline in pipelines:
        reroute_dest = ""
        processors = pipelines[pipeline].get("processors", [])
        for processor in processors:
            reroute_dest = processor.get("reroute", {}).get("destination", "N/A")
        reformatted_pipelines.append({
            "name": pipeline,
            "reroute dest": reroute_dest,
            "has_processors": "Yes" if len(processors) > 0 else "No"
        })
    tabulated_pipelines = tabulate(reformatted_pipelines, headers="keys", tablefmt="pretty")
    print(tabulated_pipelines)

def tabulate_dashboards(dashboards: list):
    """
    dashboards: list of dictionaries
    """
    # order the list of dictionaries by the 'updated_at' key
    dashboards = sorted(dashboards, key=lambda x: x.get("updated_at", ""), reverse=True)

    reformatted_dashboards = []
    reformatted_visualizations = []
    reformatted_index_patterns = []

    for dashboard in dashboards:
        attr = dashboard.get("attributes", {})
        _type = dashboard.get("type", "")
        name = attr.get("title", "")  # Assuming 'title' is the correct key for the name
        updated_at = dashboard.get("updated_at", "")
        dashboard_id = dashboard.get("id", "")


        # Check if all necessary information is available before appending
        if name and _type == "dashboard" and dashboard_id:
            reformatted_dashboards.append({
                "id": dashboard_id,
                "name": name,
                "updated_at": updated_at,
                "type": _type
            })
        if name and _type == "visualization" and dashboard_id:
            reformatted_visualizations.append({
                "id": dashboard_id,
                "name": name,
                "updated_at": updated_at,
                "type": _type
            })
        if name and _type == "index-pattern" and dashboard_id:
            reformatted_index_patterns.append({
                "id": dashboard_id,
                "name": name,
                "updated_at": updated_at,
                "type": _type
            })

    # Now tabulate the reformatted_dashboards list
    if reformatted_dashboards:
        tabulated_dashboards = tabulate(reformatted_dashboards, headers="keys", tablefmt="pretty")
        print(" --- DASHBOARDS ---")
        print(tabulated_dashboards)
        print("\n")
    if reformatted_visualizations:
        tabulated_visualizations = tabulate(reformatted_visualizations, headers="keys", tablefmt="pretty")
        print(" --- VISUALIZATIONS ---")
        print(tabulated_visualizations)
        print("\n")
    if reformatted_index_patterns:
        tabulated_index_patterns = tabulate(reformatted_index_patterns, headers="keys", tablefmt="pretty")
        print(" --- INDEX PATTERNS ---")
        print(tabulated_index_patterns)
        print("\n")
    elif not reformatted_dashboards and not reformatted_visualizations and not reformatted_index_patterns:
        print("[-] No objects found...")

def download_dashboards(client: Elasticsearch = None) -> dict:
    KIBANA_URI = ENV.get("KIBANA_URI", "")
    print("[*] Downloading dashboards...")

    headers = {
        "kbn-xsrf": "true",
        "Content-Type": "application/json"
    }

    data = {
        "type": "*",
        "includeReferencesDeep": True,
    }

    response = requests.post(
        f"{KIBANA_URI}/api/saved_objects/_export",
        headers=headers,
        auth=(USERNAME, PASSWORD),
        json=data,
        timeout=10000,
        verify=True
    )
    dashboards = response.text.encode("utf-8")
    # dashboards = json.loads(dashboards)

    with open(os.path.join(BASE_DIR, "stored_objects", "dashboards", "dashboards.ndjson"), "wb") as f:
        f.write(dashboards)
    print("[+] Dashboards downloaded successfully...")
    # Parse NDJSON text to a list of dictionaries
    dashboards = [json.loads(dashboard) for dashboard in dashboards.decode("utf-8").splitlines() if dashboard.strip()]

    return dashboards


if __name__ == "__main__":
    ES_URL = ENV.get("ES_URL", "")
    KIBANA_URI = ENV.get("KIBANA_URI", "")
    from elastic_manager import setup_auth

    authenticated_es_client = setup_auth(ELASTIC_ENDPOINT=ES_URL)
    authenticated_kibana_client = setup_auth(ELASTIC_ENDPOINT=KIBANA_URI)
    print("[*] Downloading pipelines...")
    # results = download_pipelines(client=ES_URL)
    # print("[+] Pipelines downloaded successfully...")
    print("[*] Downloading dashboards...") 
    dashboards = download_dashboards(client=authenticated_kibana_client)
    # with open(os.path.join(BASE_DIR, "stored_objects", "dashboards", "dashboards.ndjson"), "wb") as f:
    #     f.write(dashboards)
    print("[+] Dashboards downloaded successfully...")
