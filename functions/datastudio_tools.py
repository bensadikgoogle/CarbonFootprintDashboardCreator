def generate_datastudio_url(
    dashboard_id, 
    alias_connection,
    project_id, 
    dataset_id, 
    table_id,
    new_dashboard_name = "MyAdvancedCarbonFootprintDashboard"
    ):

    """
    Function used to generate final dashboard URL consuming the data from the view.
    """

    base_url=f"https://datastudio.google.com/reporting/create?c.reportId={dashboard_id}&r.reportName={new_dashboard_name}"
    parameters_url = f"&ds.{alias_connection}.connector=bigQuery&ds.{alias_connection}.projectId={project_id}&ds.{alias_connection}.type=TABLE&ds.{alias_connection}.datasetId={dataset_id}&ds.{alias_connection}.tableId={table_id}"
    
    return base_url+parameters_url
