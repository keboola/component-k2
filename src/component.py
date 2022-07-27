import logging
import requests
import paramiko
import base64
import binascii
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.component.dao import TableMetadata
from sshtunnel import SSHTunnelForwarder
from keboola.csvwriter import ElasticDictWriter
from typing import List, Dict
from io import StringIO
from client import K2Client, K2ClientException

KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_DATA_OBJECT = "data_object"
KEY_FIELDS = "fields"
KEY_CONDITIONS = "conditions"
KEY_SOURCE_URL = "source_url"
KEY_SERVICE_NAME = "service_name"
KEY_INCREMENTAL = "incremental"

KEY_USE_SSH = "use_ssh"
KEY_SSH = "ssh"
KEY_SSH_PRIVATE_KEY = "#private_key"
KEY_SSH_USERNAME = "username"
KEY_SSH_TUNNEL_HOST = "tunnel_host"
KEY_SSH_REMOTE_ADDRESS = "remote_address"
KEY_SSH_REMOTE_PORT = "remote_port"

KEY_STATE_PREVIOUS_COLUMNS = "previous_columns"

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_PASSWORD]
REQUIRED_IMAGE_PARS = []

LOCAL_BIND_ADDRESS = "localhost"
LOCAL_BIND_PORT = 9800


class Component(ComponentBase):

    def __init__(self):
        self.ssh_server = None
        super().__init__()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters

        username = params.get(KEY_USERNAME)
        password = params.get(KEY_PASSWORD)
        data_object = params.get(KEY_DATA_OBJECT)
        if fields := params.get(KEY_FIELDS):
            fields = fields.replace(" ", "")
        conditions = params.get(KEY_CONDITIONS)
        service_name = params.get(KEY_SERVICE_NAME)

        state = self.get_state_file()
        previous_columns = state.get(KEY_STATE_PREVIOUS_COLUMNS, {}).get(data_object)
        if not previous_columns:
            previous_columns = []

        if params.get(KEY_USE_SSH):
            self.create_ssh_tunnel()
            self.ssh_server.start()

        k2_address = self.get_k2_address()

        client = K2Client(username, password, k2_address, service_name)
        logging.info(f"Fetching data for object {data_object}")

        primary_keys = []
        object_meta = self.get_object_metadata(client, data_object)
        if primary_keys_list := object_meta.get("PrimaryKeyFieldList"):
            primary_keys = [primary_key.get("FieldName") for primary_key in primary_keys_list]

        logging.info(f"Primary Keys are : {primary_keys}")

        logging.info(client.estimate_amount_of_pages(data_object, self.get_id(primary_keys), fields, conditions))

        table = self.create_out_table_definition(f"{data_object}.csv", primary_key=primary_keys,
                                                 incremental=params.get(KEY_INCREMENTAL, True))
        elastic_writer = ElasticDictWriter(table.full_path, previous_columns)

        self.fetch_and_write_data(client, data_object, fields, conditions, elastic_writer)

        table.columns = elastic_writer.fieldnames

        table.table_metadata = self.generate_table_metadata(metadata=object_meta,
                                                            table_columns=elastic_writer.fieldnames)

        elastic_writer.close()
        self.write_manifest(table)

    def fetch_and_write_data(self, client: K2Client, data_object: str, fields: str, conditions: str,
                             elastic_writer: ElasticDictWriter) -> None:
        try:
            for i, page_data in enumerate(client.get_object_data(data_object, fields, conditions)):
                if i % 100 == 0:
                    logging.info(f"Fetching page {i + 1}")
                parsed_data = self.parse_object_data(page_data)
                elastic_writer.writerows(parsed_data)
        except K2ClientException as k2_exc:
            raise UserException(k2_exc) from k2_exc
        except requests.exceptions.HTTPError as http_exc:
            raise UserException(http_exc) from http_exc

    def parse_object_data(self, data: List) -> List:
        parsed_data = []
        for row in data:
            parsed_row = self.parse_object(row)
            parsed_data.append(parsed_row)
        return parsed_data

    def create_ssh_tunnel(self) -> None:
        params = self.configuration.parameters
        ssh = params.get(KEY_SSH)
        private_key = self.get_private_key(ssh.get(KEY_SSH_PRIVATE_KEY))
        ssh_tunnel_host = ssh.get(KEY_SSH_TUNNEL_HOST)
        ssh_remote_address = ssh.get(KEY_SSH_REMOTE_ADDRESS)
        try:
            ssh_remote_port = int(ssh.get(KEY_SSH_REMOTE_PORT))
        except ValueError as v_e:
            raise UserException("Remote port must be a valid integer") from v_e
        ssh_username = ssh.get(KEY_SSH_USERNAME)

        self.ssh_server = SSHTunnelForwarder(ssh_address_or_host=ssh_tunnel_host,
                                             ssh_pkey=private_key,
                                             ssh_username=ssh_username,
                                             remote_bind_address=(ssh_remote_address, ssh_remote_port),
                                             local_bind_address=(LOCAL_BIND_ADDRESS, LOCAL_BIND_PORT),
                                             ssh_config_file=None,
                                             allow_agent=False)

    def get_k2_address(self) -> str:
        params = self.configuration.parameters
        if params.get(KEY_USE_SSH):
            return f"http://{LOCAL_BIND_ADDRESS}:{LOCAL_BIND_PORT}"
        source_url = params.get(KEY_SOURCE_URL)
        if "https" not in source_url or "http" not in source_url:
            raise UserException("Source Url should contain scheme : http or https")
        return source_url

    @staticmethod
    def get_id(primary_keys: List[str]) -> str:
        if "Id" in primary_keys:
            return "Id"
        for primary_key in primary_keys:
            if "id" in primary_key.lower():
                return primary_key
        if primary_keys:
            return primary_keys[0]

    @staticmethod
    def validate_ssh_private_key(ssh_private_key: str) -> None:
        if "BEGIN OPENSSH PRIVATE KEY" not in ssh_private_key:
            raise UserException("SSH Private key is invalid, "
                                "make sure it contains the string BEGIN OPENSSH PRIVATE KEY")
        if "\n" not in ssh_private_key:
            raise UserException("SSH Private key is invalid, "
                                "make sure it \\n characters as new lines")

    def get_private_key(self, b64_input_key):
        try:
            input_key = base64.b64decode(b64_input_key, validate=True).decode('utf-8')
        except binascii.Error as bin_err:
            raise UserException(f'Failed to base64-decode the private key,'
                                f' confirm you have base64-encoded your private key input variable. '
                                f'Detail: {bin_err}') from bin_err
        self.validate_ssh_private_key(input_key)
        try:
            return paramiko.RSAKey.from_private_key(StringIO(input_key))
        except paramiko.ssh_exception.SSHException as pkey_error:
            raise UserException("Invalid private key")from pkey_error

    def parse_object(self, data_object, parent_key: str = "") -> Dict:
        parsed_object = {}
        for field in data_object.get("FieldValues"):
            key = self._construct_key(parent_key, "_", field.get('Name'))
            if isinstance(field.get("Value"), dict):
                parsed_object.update(self.parse_object(field.get("Value"), parent_key=key))
            else:
                parsed_object[key] = field.get("Value")
        return parsed_object

    @staticmethod
    def _construct_key(parent_key, separator, child_key):
        return "".join([parent_key, separator, child_key]) if parent_key else child_key

    @staticmethod
    def get_object_metadata(client: K2Client, data_object: str) -> Dict:
        try:
            return client.get_object_meta(data_object)
        except K2ClientException as k2exc:
            raise UserException(k2exc) from k2exc

    @staticmethod
    def generate_table_metadata(metadata: Dict, table_columns: List[str]) -> TableMetadata:
        tm = TableMetadata()
        if caption := metadata.get("Caption"):
            tm.add_table_description(caption)
        column_descriptions = {}
        for column in metadata.get("FieldList", {}):
            if column.get("FieldName") in table_columns:
                column_descriptions[column.get("FieldName")] = column.get("Description", "")
        tm.add_column_descriptions(column_descriptions)
        return tm


if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
