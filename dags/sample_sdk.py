import pathlib

import doublecloud
from doublecloud.clickhouse.v1.cluster_service_pb2 import ListClustersRequest
from doublecloud.clickhouse.v1.cluster_service_pb2_grpc import ClusterServiceStub

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook


@dag(
    dag_id=pathlib.Path(__file__).stem,
    dag_display_name="List ClickHouse clusters using SDK and passed service account",
    tags=["sample", "clickhouse", "sdk", "service_account"],
    schedule=None,
    catchup=False,
)
def sample_list_ch_clusters():
    @task
    def get_project_id():
        '''
        What project to use?
        '''
        return "cloud"


    @task
    def display_clusters(project_id):
        '''
        Lists CH clusters using the SDK
        '''
        # Fetch the connection using Airflow's connection management system
        # To use the functionality, go to Cluster Settings and specify a Service Account
        connection = BaseHook.get_connection('doublecloud_api_private_key')

        # Setup SDK using data from the connection
        sdk = doublecloud.SDK(service_account_key={
            "id": connection.extra_dejson.get('kid'),
            "service_account_id": connection.login,
            "private_key": connection.password,
        })

        cluster_service = sdk.client(ClusterServiceStub)
        response = cluster_service.List(ListClustersRequest(
            project_id=project_id,
        ))
        print("Your CH clusters are:")
        for cluster in response.clusters:
            print(cluster)

    display_clusters(project_id=get_project_id())


my_dag = sample_list_ch_clusters()


if __name__ == '__main__':
    my_dag.test()
