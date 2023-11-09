from kubernetes.client import models as k8s
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow'
}

 
with DAG(
    dag_id='DockerPrivateImageKubePod',
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=days_ago(2),
    catchup=False,
    tags=['k8s-pod-operator','docker private image repo'],
) as dag:

    k = KubernetesPodOperator(
        namespace='default',
        image="ambikakubernetesclusterregistry.azurecr.io/samples/hello-world:latest",
        image_pull_secrets=[k8s.V1LocalObjectReference("testquay")],        
        name="k8s-pod",
        task_id="task",
        is_delete_operator_pod=True,
        hostnetwork=False,
        startup_timeout_seconds=1000
    )

