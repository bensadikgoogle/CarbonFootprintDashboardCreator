from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os

# Import template of the query to generate the custom view
current_wd = os.getcwd()
query_path = os.path.join(
    current_wd, 
    "resources/view_template.sql"
)

def dataset_exists(
    bq_client, 
    dataset_id 
    ):
    """
    Check dataset existence for the following id structure: {project_id}.{dataset_id}.
    """
    try:
        bq_client.get_dataset(dataset_id) 
        print(
            f"Dataset {dataset_id} already exists so skipping creation."
        )
        return True
    except NotFound:
        print(
            f"Dataset {dataset_id} is not found so creating it."
        )
        return False

def create_dataset(
    bq_client, 
    VIEW_PROJECT,
    VIEW_DATASET, 
    BILLING_PROJECT, 
    BILLING_DATASET, 
    CARBON_PROJECT, 
    CARBON_DATASET
    ):

    """
    Worklow to check and eventually create required datasets
    """

    if not dataset_exists(
        bq_client,
        f"{BILLING_PROJECT}.{BILLING_DATASET}"
    ):
        raise NotFound(f"Dataset {BILLING_PROJECT}.{BILLING_DATASET} is not found, please make sure it exists.")

    if not dataset_exists(
        bq_client,
        f"{CARBON_PROJECT}.{CARBON_DATASET}"
    ):
        raise NotFound(f"Dataset {CARBON_PROJECT}.{CARBON_DATASET} is not found, please make sure it exists.")
    
    # Retrieving Billing and Carbon dataset
    billing_dataset_info = bq_client.get_dataset(
        f"{BILLING_PROJECT}.{BILLING_DATASET}"
    )

    carbon_dataset_info = bq_client.get_dataset(
            f"{CARBON_PROJECT}.{CARBON_DATASET}"
    )

    if dataset_exists(
        bq_client,
        f"{VIEW_PROJECT}.{VIEW_DATASET}"
    ):
        # If the final dataset already exists, check that all datasets are in the same location
        view_dataset_info = bq_client.get_dataset(
            f"{VIEW_PROJECT}.{VIEW_DATASET}"
        )
        
        if not (view_dataset_info.location == billing_dataset_info.location == carbon_dataset_info.location):
            raise ValueError("All datasets need to be in the same location.")
    else:
        if not (billing_dataset_info.location == carbon_dataset_info.location):
            raise ValueError("Billing and carbon datasets need to be in the same location to create the final view.")
        
        # Create the View dataset object if it does not already exist
        dataset = bigquery.Dataset(
            f"{VIEW_PROJECT}.{VIEW_DATASET}"
        )
        dataset.location = carbon_dataset_info.location
        print(
            f"Final view will be created in {carbon_dataset_info.location}."
        )

        dataset = bq_client.create_dataset(dataset, timeout=30)

        print(
            f"Created dataset {VIEW_PROJECT}.{VIEW_DATASET}."
        )

def create_final_view(
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
    CURRENCY):

    """
    Function that:
    1. Forges custom query based on user's input
    2. Use the custom query to create a view in BigQuery using the input name
    """

    query = open(
        query_path,
        "r"
        ).read()

    # Replacing view fields
    query = query.replace(
        "$VIEW_PROJECT_ID", 
        VIEW_PROJECT
    ).replace(
        "$VIEW_DATASET", 
        VIEW_DATASET
    ).replace(
        "$VIEW_NAME", 
        VIEW_NAME
    )

    # Replacing billing tabke fields
    query = query.replace(
        "$BILLING_PROJECT_ID", 
        BILLING_PROJECT
    ).replace(
        "$BILLING_DATASET", 
        BILLING_DATASET
    ).replace(
        "$BILLING_TABLE", 
        BILLING_TABLE
    )

    # Replacing carbon table fields
    query = query.replace(
        "$CARBON_PROJECT_ID", 
        CARBON_PROJECT
    ).replace(
        "$CARBON_DATASET", 
        CARBON_DATASET
    ).replace(
        "$CARBON_TABLE", 
        CARBON_TABLE
    )

    # Replacing the currency field 
    query = query.replace(
        "$CURRENCY", 
        CURRENCY
    )

    bq_view_client = bigquery.Client(
        project = VIEW_PROJECT
    )

    job = bq_view_client.query(
        query
    )
    job.result()

    print(
        f"Created view {VIEW_PROJECT}.{VIEW_DATASET}.{VIEW_NAME}."
    )


