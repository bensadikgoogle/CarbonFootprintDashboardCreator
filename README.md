# Advanced Carbon Footprint Dashboard

If you are part of a Carbon/Green Ops team in charge with monitoring carbon emissions in GCP, this dashboard will empower you by allowing you to dive in your carbon data easily in Data Studio.

## Prerequisites

This dashboard assumes that the user has already configured exports to BigQuery for the billing data and carbon footprint data and has access to this table.

**Note that this is required for the BigQuery datasets to be in the same GCP global region for this script to work.**

Follow these links to configure the exports:
* [Carbon footprint export to BigQuery](https://cloud.google.com/carbon-footprint/docs/export)
* [Billing account data export to BigQuery](https://cloud.google.com/billing/docs/how-to/export-data-bigquery#setup)

## Run the walkthrough tutorial

### 1. Click on the following button
This will open an ephemeral Cloud Shell window with your credentials and this repository already cloned in the environment. 

[![Open in Cloud Shell](https://gstatic.com/cloudssh/images/open-btn.svg)](https://shell.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/bensadikgoogle/CarbonFootprintDashboardCreator.git)

### 2. Install required Python packages
`
pip3 -r requirements.txt
`

### 3. Run the script

There are two different options to create the BigQuery View and Data Studio URL.

#### Using a configuration file 

Run the following command in the Cloud Shell environment

`
python3 main.py -f config.json
`

Configuration file structure:

`
config.json
`
```javascript
{
    "VIEW_PROJECT" : "", 
    "VIEW_DATASET" : "", 
    "VIEW_NAME" : "", 
    "BILLING_PROJECT" : "",
    "BILLING_DATASET" : "",
    "BILLING_TABLE" : "",
    "CARBON_PROJECT" : "",
    "CARBON_DATASET" : "",
    "CARBON_TABLE" : "",
    "CURRENCY": "" 
}
```

#### Without using a configuration file

Run the following command in the Cloud Shell environment : 

`
python3 main.py -cp CARBON_PROJECT -cd CARBON_DATASET -ct CARBON_TABLE -bp BILLING_PROJECT -bd BILLING_DATASET -bt BILLING_TABLE -vp VIEW_PROJECT -vd VIEW_DATASET -vn VIEW_NAME -c CURRENCY
`

**
Explanation of each field:
**

Field | Description
--- | ---
CARBON_PROJECT  | Project id of carbon export
CARBON_DATASET | Dataset id of carbon export
CARBON_TABLE | Table id of carbon export
BILLING_PROJECT | Project id of billing export
BILLING_DATASET | Dataset id of billing export
BILLING_TABLE  | Table id of billing export
VIEW_PROJECT  | Project id of final view
VIEW_DATASET  | Dataset id of final view
VIEW_NAME  | Name of final view
CURRENCY | Currency in the billing data

### 3. Click on the link and share the dashboard with your team
