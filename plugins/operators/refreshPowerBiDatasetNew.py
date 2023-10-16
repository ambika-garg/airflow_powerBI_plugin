from typing import Optional, Union
from airflow.models import BaseOperator
import requests
from airflow.exceptions import AirflowException
from azure.identity import ClientSecretCredential, DefaultAzureCredential

Credentials = Union[ClientSecretCredential, DefaultAzureCredential]

class PowerBIDatasetRefreshOperator(BaseOperator):
    def __init__(
        self,
        dataset_id: str,
        group_id: Optional[bool] = None, 
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.group_id = group_id

    def execute(self, context):
        """
        Refresh the Power BI Dataset
        """
        self.refresh_dataset(self.dataset_id)

    def refresh_dataset(self, dataset_id: str) -> None:
        """
        Triggers a refresh for the specified dataset from "My Workspace".
        """
        url = f'https://api.powerbi.com/v1.0/myorg'

        # Add the dataset key
        url += f'/datasets/{dataset_id}/refreshes'

        self._send_request(url=url)

    def _send_request(self,
                      url: str,
                      **kwargs) -> requests.Response:
        """
        Send a request to the Power BI REST API.

        :param url: The URL against which the request needs to be made.
        :return: requests.Response
        """

        self.header = {'Authorization': f'Bearer {self._get_token()}'}

        r = requests.post(url=url, headers=self.header, **kwargs)
        r.raise_for_status()
        return r

    def _get_token(self) -> str:
        """
        Retrieve the `access token` used to authenticate against the API.
        """

        credential: Credentials
        credential = ClientSecretCredential(
            client_id="67ae3905-1d42-404e-a20a-b6d51b28617c",
            client_secret="eBe8Q~n8DK-xQVB2_VsRmmU1SGZ.Qm6kN.zpgcfw",
            tenant_id="72f988bf-86f1-41af-91ab-2d7cd011db47"
        )

        self.log.info(credential)
        return ""