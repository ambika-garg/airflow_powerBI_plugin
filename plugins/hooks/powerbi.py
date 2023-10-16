# from airflow.hooks.base_hook import BaseHook
# from typing import Optional, Union, Any
# from azure.identity import ClientSecretCredential, DefaultAzureCredential

# Credentials = Union[ClientSecretCredential, DefaultAzureCredential]

# def get_field(extras: dict, field_name: str, strict: bool = False):
#     """Get field from extra, first checking short name, then for backcompat we check for prefixed name."""
#     backcompat_prefix = "extra__azure_data_factory__"
#     if field_name.startswith("extra__"):
#         raise ValueError(
#             f"Got prefixed name {field_name}; please remove the '{backcompat_prefix}' prefix "
#             "when using this method."
#         )
#     if field_name in extras:
#         return extras[field_name] or None
#     prefixed_name = f"{backcompat_prefix}{field_name}"
#     if prefixed_name in extras:
#         return extras[prefixed_name] or None
#     if strict:
#         raise KeyError(f"Field {field_name} not found in extras")

# class PowerBI(BaseHook):
#     """
#     A hook to interact with Power BI.
#     """

#     @staticmethod
#     def get_connection_form_widgets() -> dict[str, Any]:
#         """Returns connection widgets to add to connection form."""
#         from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
#         from flask_babel import lazy_gettext
#         from wtforms import StringField

#         return {
#             "tenantId": StringField(lazy_gettext("Tenant ID"), widget=BS3TextFieldWidget()),
#             "subscriptionId": StringField(lazy_gettext("Subscription ID"), widget=BS3TextFieldWidget())
#         }

#     @staticmethod
#     def get_ui_field_behaviour() -> dict[str, Any]:
#         """Returns custom field behaviour."""
#         return {
#             "hidden_fields": ["schema", "port", "host", "extra"],
#             "relabeling": {
#                 "login": "Client ID",
#                 "password": "Client Secret",
#             },
#         }
    
#     def get_conn(self) -> DataFactoryManagementClient:
#         if self._conn is not None:
#             return self._conn

#         conn = self.get_connection(self.conn_id)
#         extras = conn.extra_dejson
#         tenant = get_field(extras, "tenantId")

#         try:
#             subscription_id = get_field(extras, "subscriptionId", strict=True)
#         except KeyError:
#             raise ValueError("A Subscription ID is required to connect to Azure Data Factory.")

#         credential: Credentials
#         if conn.login is not None and conn.password is not None:
#             if not tenant:
#                 raise ValueError("A Tenant ID is required when authenticating with Client ID and Secret.")

#             credential = ClientSecretCredential(
#                 client_id=conn.login, client_secret=conn.password, tenant_id=tenant
#             )
#         else:
#             credential = DefaultAzureCredential()
#         self._conn = self._create_client(credential, subscription_id)

#         return self._conn
