from datetime import timedelta
from textwrap import dedent
from kubernetes.client import models as k8s
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'python_kubernetes_workflow',
    default_args=default_args,
    description='python_kubernetes_workflow',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['python_kubernetes_workflow'],
) as dag:
    t1 = KubernetesPodOperator(
        namespace='adf',
        image='ambikakubernetesclusterregistry.azurecr.io/azuredocs/aci-helloworld:latest',
        image_pull_policy = 'Always',
        image_pull_secrets=[k8s.V1LocalObjectReference("sample-secret")],
        name="task-1",
        is_delete_operator_pod=True,
        in_cluster=False,
        task_id="task-1",
        get_logs=True
    )

    t1
