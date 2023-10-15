from typing import Optional
from airflow.models import BaseOperator
import requests
from airflow.exceptions import AirflowException

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
        self.refresh_dataset(self.dataset_id, self.group_id)

    def refresh_dataset(self, dataset_id: str, group_id: str = None) -> None:
        """
        Triggers a refresh for the specified dataset from "My Workspace" if
        no `group id` is specified or from the specified workspace when
        `group id` is specified.

        https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/refreshdataset
        https://docs.microsoft.com/en-us/rest/api/power-bi/datasets/refreshdatasetingroup

        :param dataset_id: The dataset id.
        :param group_id: The workspace id.
        """
        url = f'https://api.powerbi.com/v1.0/myorg'

        # add the group id if it is specified
        if group_id:
            url += f'/groups/{group_id}'

        # add the dataset key
        url += f'/datasets/{dataset_id}/refreshes'

        self._send_request('POST', url=url)

    def _send_request(self,
                      request_type: str,
                      url: str,
                      **kwargs) -> requests.Response:
        """
        Send a request to the Power BI REST API.

        This method checks to see if authorisation token has been retrieved and
        the request `header` has been built using it. If not then it will
        establish the connection to perform this action on the first call. It
        is important to NOT have this connection established as part of the
        initialisation of the hook to prevent a Power BI API call each time
        the Airflow scheduler refreshes the DAGS.


        :param request_type: Request type (GET, POST, PUT etc.).
        :param url: The URL against which the request needs to be made.
        :return: requests.Response
        """
        # if not self.header:
        self.header = {'Authorization': f'Bearer {self._get_token()}'}

        request_funcs = {
            'GET': requests.get,
            'POST': requests.post
        }

        func = request_funcs.get(request_type.upper())

        if not func:
            raise AirflowException(
                f'Request type of {request_type.upper()} not supported.'
            )

        r = func(url=url, headers=self.header, **kwargs)
        r.raise_for_status()
        return r

    def _get_token(self) -> str:
        """
        Retrieve the `access token` used to authenticate against the API.
        """
        return "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSIsImtpZCI6IjlHbW55RlBraGMzaE91UjIybXZTdmduTG83WSJ9.eyJhdWQiOiJodHRwczovL2FuYWx5c2lzLndpbmRvd3MubmV0L3Bvd2VyYmkvYXBpIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3LyIsImlhdCI6MTY5NzM3NzI4NiwibmJmIjoxNjk3Mzc3Mjg2LCJleHAiOjE2OTczODI0MjIsImFjY3QiOjAsImFjciI6IjEiLCJhaW8iOiJBWVFBZS84VUFBQUFzQUdBVEFmSkZ0K3VTdjA4Y3pidVVmbmJtY3pZbE5NcTJxQlNrNnhvVXErb2h3QlNMMzExV0J2QVUzbmk2ajlBOVZqNWlSWDlyWW11NVdSTWNFQm1iU2ljdmZlVjhxaUs1U1Nkdmpmd0lxd3R2N0ZONlZOUnAzTmVRalJaWTl2Sit5SjJUWVk1REFyd3B6cWxZNnlRZEl1ZGc5ZllrcGtScVN2OTZQYTIvQkE9IiwiYW1yIjpbInB3ZCIsImZpZG8iLCJyc2EiLCJtZmEiXSwiYXBwaWQiOiIxOGZiY2ExNi0yMjI0LTQ1ZjYtODViMC1mN2JmMmIzOWIzZjMiLCJhcHBpZGFjciI6IjAiLCJjb250cm9scyI6WyJhcHBfcmVzIl0sImNvbnRyb2xzX2F1ZHMiOlsiMDAwMDAwMDktMDAwMC0wMDAwLWMwMDAtMDAwMDAwMDAwMDAwIiwiMDAwMDAwMDMtMDAwMC0wZmYxLWNlMDAtMDAwMDAwMDAwMDAwIl0sImRldmljZWlkIjoiMjFlMmFjODQtNzcyNC00MDU1LTk4MGEtOTc1ZjRhOWY4MzIxIiwiZmFtaWx5X25hbWUiOiJHYXJnIiwiZ2l2ZW5fbmFtZSI6IkFtYmlrYSIsImlwYWRkciI6IjcxLjE2My4xNTcuMTMiLCJuYW1lIjoiQW1iaWthIEdhcmcgKEJ1Y2hlciBhbmQgQ2hyaXN0aWFuIENvbnN1bHRpbikiLCJvaWQiOiI2MmRhZDZhYi0wNTdmLTQ5NmYtODg5NS1mNWU1M2JhY2M1MDUiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMjEyNzUyMTE4NC0xNjA0MDEyOTIwLTE4ODc5Mjc1MjctNzA0NjE3OTUiLCJwdWlkIjoiMTAwMzIwMDJENjUxQkE5NyIsInJoIjoiMC5BUm9BdjRqNWN2R0dyMEdScXkxODBCSGJSd2tBQUFBQUFBQUF3QUFBQUFBQUFBQWFBUDQuIiwic2NwIjoiQXBwLlJlYWQuQWxsIENhcGFjaXR5LlJlYWQuQWxsIENhcGFjaXR5LlJlYWRXcml0ZS5BbGwgQ29udGVudC5DcmVhdGUgRGFzaGJvYXJkLlJlYWQuQWxsIERhc2hib2FyZC5SZWFkV3JpdGUuQWxsIERhdGFmbG93LlJlYWQuQWxsIERhdGFmbG93LlJlYWRXcml0ZS5BbGwgRGF0YXNldC5SZWFkLkFsbCBEYXRhc2V0LlJlYWRXcml0ZS5BbGwgR2F0ZXdheS5SZWFkLkFsbCBHYXRld2F5LlJlYWRXcml0ZS5BbGwgUGlwZWxpbmUuRGVwbG95IFBpcGVsaW5lLlJlYWQuQWxsIFBpcGVsaW5lLlJlYWRXcml0ZS5BbGwgUmVwb3J0LlJlYWQuQWxsIFJlcG9ydC5SZWFkV3JpdGUuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWQuQWxsIFN0b3JhZ2VBY2NvdW50LlJlYWRXcml0ZS5BbGwgVGVuYW50LlJlYWQuQWxsIFRlbmFudC5SZWFkV3JpdGUuQWxsIFVzZXJTdGF0ZS5SZWFkV3JpdGUuQWxsIFdvcmtzcGFjZS5SZWFkLkFsbCBXb3Jrc3BhY2UuUmVhZFdyaXRlLkFsbCIsInNpZ25pbl9zdGF0ZSI6WyJkdmNfbW5nZCIsImR2Y19jbXAiLCJrbXNpIl0sInN1YiI6IlBkN051cjFuZmh2UTFzckFPWmExMThQUmxxT0ZBVi1RUXAxcVJxRVhKY1EiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1bmlxdWVfbmFtZSI6InYtYW1iaWthZ2FyZ0BtaWNyb3NvZnQuY29tIiwidXBuIjoidi1hbWJpa2FnYXJnQG1pY3Jvc29mdC5jb20iLCJ1dGkiOiI5NmdvWV9xZ2lVQ1BOTVZsZjNHdkFBIiwidmVyIjoiMS4wIiwid2lkcyI6WyJiNzlmYmY0ZC0zZWY5LTQ2ODktODE0My03NmIxOTRlODU1MDkiXX0.AIXKC72q1dFp5pZu-KXK8paHHou3ZngxS9Ayl3MTW6jUz7It27iwLyLlozACUk8EdH1DCzWxYlgCcdFYQFqdmhBydf4Sq1KvG696wjZsWHcQr7fUv0s_nf3eyo9WhKiXwuCgnDQFfk5FbLnvbQUFZFE6pj0P-P_uNncAXUEblGcafNYqXFSsB-7tHshpxOwc8p3MdGhe2i9rOKk-o-SpWbymtzZhf2ktLWYmTltXJMMSwe4CLe0D0KItgAMDEeD_DI8owYUDIVGlxhNF5oBFxyyu7gJlpX_8DVD089qgl1Q7d2AwhGS1AA58kjz5D-jar4Fm4lByhccJWtkZkLIM0w"