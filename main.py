from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import argparse, sys



### INPUT 
dashboard_id_path = "dashboard_id.txt"
new_dashboard_name = "my_little_test"
alias_connection = "ds69"
project_id = "benjaminsadik-carbonfootprint"
dataset_id = "carbonfootprintUSA" 
table_id ="carbon_billing_export_USD_view"

## DEDUCE 
dashboard_id = open(
    dashboard_id_path,
    "r"
).read()
view_url = f"&ds.{alias_connection}.connector=bigQuery&ds.{alias_connection}.projectId={project_id}&ds.{alias_connection}.type=TABLE&ds.{alias_connection}.datasetId={dataset_id}&ds.{alias_connection}.tableId={table_id}"
base_url=f"https://datastudio.google.com/reporting/create?c.reportId={dashboard_id}&r.reportName={new_dashboard_name}"

dashboard_id = open(
    dashboard_id_path,
    "r"
).read()

bq_client = bigquery.Client()

def dataset_exists(dataset_id):
    try:
        bq_client.get_dataset(dataset_id)  # Make an API request.
        print(f"Dataset {dataset_id} already exists so skipping creation.")
        return True
    except NotFound:
        print(f"Dataset {dataset_id} is not found so creating it.")
        return False

def create_dataset(
    VIEW_PROJECT,
    VIEW_DATASET, 
    BILLING_PROJECT, 
    BILLING_DATASET, 
    CARBON_PROJECT, 
    CARBON_DATASET
):
    if not dataset_exists(f"{BILLING_PROJECT}.{BILLING_DATASET}"):
        raise NotFound(f"Dataset {BILLING_PROJECT}.{BILLING_DATASET} is not found, please make sure it exists.")

    if not dataset_exists(f"{CARBON_PROJECT}.{CARBON_DATASET}"):
        raise NotFound(f"Dataset {CARBON_PROJECT}.{CARBON_DATASET} is not found, please make sure it exists.")
    
    view_dataset_info = bq_client.get_dataset(
        f"{VIEW_PROJECT}.{VIEW_DATASET}"
    )
    billing_dataset_info = bq_client.get_dataset(
        f"{BILLING_PROJECT}.{BILLING_DATASET}"
    )

    if dataset_exists(f"{VIEW_PROJECT}.{VIEW_DATASET}"):
        # If the final dataset already exists, check that all datasets are in the same location
        carbon_dataset_info = bq_client.get_dataset(
            f"{CARBON_PROJECT}.{CARBON_DATASET}"
        )
        
        if not (view_dataset_info.location == billing_dataset_info.location == carbon_dataset_info.location):
            raise ValueError("All datasets need to be in the same location.")

        return
    else:
        if not (billing_dataset_info.location == carbon_dataset_info.location):
            raise ValueError("Billing and carbon datasets need to be in the same location to create the final view.")
        
        dataset = bigquery.Dataset(
            f"{VIEW_PROJECT}.{VIEW_DATASET}"
        )
        dataset.location = carbon_dataset_info.location
        print(
            f"Final view will be created in {carbon_dataset_info.location}."
        )

        dataset = bq_client.create_dataset(dataset, timeout=30)

        print(
            f"Created dataset {VIEW_PROJECT}.{VIEW_DATASET}"
        )

def create_final_view():
    return 

def main(argv):
    parser=argparse.ArgumentParser(
        description="Billing and carbon export information"
    )
    parser.add_argument(
        "-f",
        dest="CONFIG_FILE", 
        type=str, 
        help="Configuration file"
    )
    parser.add_argument(
        "-pc",
        dest="CARBON_PROJECT", 
        type=str, 
        help="Project id of carbon export"
    )
    parser.add_argument(
        "-dc",
        dest="CARBON_DATASET", 
        type=str, 
        help="Dataset id of carbon export"
    )
    parser.add_argument(
        "-tc",
        dest="CARBON_TABLE", 
        type=str, 
        help="Table id of carbon export"
    )
    parser.add_argument(
        "-pb",
        dest="BILLING_PROJECT", 
        type=str, 
        help="Project id of billing export"
    )
    parser.add_argument(
        "-db",
        dest="BILLING_DATASET", 
        type=str, 
        help="Dataset id of billing export"
    )
    parser.add_argument(
        "-tb",
        dest="BILLING_TABLE", 
        type=str, 
        help="Table id of billing export"
    )

    parser.add_argument(
        "-pv",
        dest="VIEW_PROJECT", 
        type=str, 
        help="Project id of final view"
    )
    parser.add_argument(
        "-dv",
        dest="VIEW_DATASET", 
        type=str, 
        help="Dataset id of final view"
    )

    parser.add_argument(
        "-tv",
        dest="VIEW_TABLE", 
        type=str, 
        help="Table id of final view"
    )

    parser.add_argument(
        "-c",
        dest ="CURRENCY", 
        type=str, 
        help="Currency in the billing data"
    )


print(create_dataset(project_id, dataset_id, project_id, dataset_id, project_id, dataset_id))