import datetime
from airflow import DAG

from operators.refreshPowerBiDatasetNew import PowerBIDatasetRefreshOperator

with DAG(
    dag_id="refresh_powerbi-dataset",
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["pipeline"],
) as dag:
    trigger_refresh = PowerBIDatasetRefreshOperator(
        dataset_id = "af05a415-42ca-49b5-aee4-ac89dcb71a40",
        task_id = "dataset_refresh_task"
    )
    
    # (
    #     azure_synapse_conn_id="azure_synapse_connection",
    #     task_id="trigger_refresh",
    #     pipeline_name="Pipeline 1",
    #     azure_synapse_workspace_dev_endpoint="https://ambika-synapse-workspace.dev.azuresynapse.net",
    #     deferrable=False 
    # )

    trigger_refresh