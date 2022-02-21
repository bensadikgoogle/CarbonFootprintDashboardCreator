from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import argparse, sys
import json
import os 
from functions import dataset_exists, create_dataset, create_final_view, generate_datastudio_url

# Import metadata about the dashboard: connection_alias, dashboard_id
current_wd = os.getcwd()
dashboard_config_path = os.path.join(
    current_wd, 
    "resources/dashboard_template_config.json"
)

bq_client = bigquery.Client()

def pipeline(
    bq_client, 
    VIEW_PROJECT,
    VIEW_DATASET, 
    VIEW_NAME,
    BILLING_PROJECT, 
    BILLING_DATASET, 
    BILLING_TABLE,
    CARBON_PROJECT, 
    CARBON_DATASET,
    CARBON_TABLE, 
    CURRENCY, 
    dashboard_id, 
    alias_connection):

    """
    Function encapsulating the complete workflow:
    1. Check datasets existence
    2. Create view joining billing and carbon export data
    3. Forge dashboard url 
    """
    
    create_dataset(
        bq_client,
        VIEW_PROJECT, 
        VIEW_DATASET, 
        BILLING_PROJECT, 
        BILLING_DATASET, 
        CARBON_PROJECT, 
        CARBON_DATASET
    )
    
    create_final_view(
        bq_client,
        VIEW_PROJECT, 
        VIEW_DATASET, 
        VIEW_NAME, 
        BILLING_PROJECT, 
        BILLING_DATASET, 
        BILLING_TABLE, 
        CARBON_PROJECT, 
        CARBON_DATASET, 
        CARBON_TABLE, 
        CURRENCY
    )

    url = generate_datastudio_url(
        dashboard_id, 
        alias_connection, 
        VIEW_PROJECT, 
        VIEW_DATASET, 
        VIEW_NAME
    )

    print(
        f"\nYour dashboard has been created, click on the following link and share it:\n\n{url}\n"
    )
    return url

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
        "-cp",
        dest="CARBON_PROJECT", 
        type=str, 
        help="Project id of carbon export"
    )
    parser.add_argument(
        "-cd",
        dest="CARBON_DATASET", 
        type=str, 
        help="Dataset id of carbon export"
    )
    parser.add_argument(
        "-ct",
        dest="CARBON_TABLE", 
        type=str, 
        help="Table id of carbon export"
    )
    parser.add_argument(
        "-bp",
        dest="BILLING_PROJECT", 
        type=str, 
        help="Project id of billing export"
    )
    parser.add_argument(
        "-bd",
        dest="BILLING_DATASET", 
        type=str, 
        help="Dataset id of billing export"
    )
    parser.add_argument(
        "-bt",
        dest="BILLING_TABLE", 
        type=str, 
        help="Table id of billing export"
    )

    parser.add_argument(
        "-vp",
        dest="VIEW_PROJECT", 
        type=str, 
        help="Project id of final view"
    )
    parser.add_argument(
        "-vd",
        dest="VIEW_DATASET", 
        type=str, 
        help="Dataset id of final view"
    )

    parser.add_argument(
        "-vn",
        dest="VIEW_NAME", 
        type=str, 
        help="Name of the final view"
    )

    parser.add_argument(
        "-c",
        dest ="CURRENCY", 
        type=str, 
        help="Currency in the billing data"
    )

    args = parser.parse_args()

    # Import dashboard config metadatas

    json_file = open(
        dashboard_config_path
    )
    dashboard_config = json.load(
        json_file
    )
    json_file.close()

    # If a file is given
    
    if args.CONFIG_FILE is not None:

        json_file = open(
            args.CONFIG_FILE
        )
        config = json.load(
            json_file
        )
        json_file.close()

        pipeline(
            bq_client,
            config["VIEW_PROJECT"],
            config["VIEW_DATASET"], 
            config["VIEW_NAME"], 
            config["BILLING_PROJECT"],
            config["BILLING_DATASET"], 
            config["BILLING_TABLE"], 
            config["CARBON_PROJECT"], 
            config["CARBON_DATASET"], 
            config["CARBON_TABLE"],
            config["CURRENCY"], 
            dashboard_config["dashboard_id"], 
            dashboard_config["alias_connection"]
        )

    else:
        pipeline(
            bq_client,
            args.VIEW_PROJECT,
            args.VIEW_DATASET, 
            args.VIEW_NAME, 
            args.BILLING_PROJECT,
            args.BILLING_DATASET, 
            args.BILLING_TABLE, 
            args.CARBON_PROJECT, 
            args.CARBON_DATASET, 
            args.CARBON_TABLE,
            args.CURRENCY, 
            dashboard_config["dashboard_id"], 
            dashboard_config["alias_connection"]
        )

if __name__ == "__main__":
   main(sys.argv[:1])
