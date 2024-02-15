import logging
import warnings
from datetime import datetime
from typing import List, Dict, Optional

import dateparser
import requests
from keboola.component.base import ComponentBase
from keboola.component.dao import TableMetadata
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError

from keboola.utils.helpers import comma_separated_values_to_list
from client import K2Client, K2ClientException
from k2parser import K2DataParser
from table_handler import TableHandler
from ssh_utils import get_private_key, SomeSSHException
from k2_object_metadata import K2ObjectMetadata, K2_OBJECT_CLASS_NAME_KEY, K2_OBJECT_FIELD_NAME_KEY, \
    K2_OBJECT_PARENT_CLASS_NAME_KEY, K2_OBJECT_PARENT_PRIMARY_KEYS

# Ignore dateparser warnings regarding pytz
warnings.filterwarnings(
    "ignore",
    message="The localize method is no longer necessary, as this time zone supports the fold attribute",
)

KEY_USERNAME = "username"
KEY_PASSWORD = "#password"
KEY_DATA_OBJECT = "data_object"
KEY_FIELDS = "fields"
KEY_CONDITIONS = "conditions"
KEY_SOURCE_URL = "source_url"
KEY_SERVICE_NAME = "service_name"

KEY_LOADING_OPTIONS = "loading_options"
KEY_LOAD_TYPE = "load_type"
KEY_INCREMENTAL_FIELD = "incremental_field"
KEY_DATE_FROM = "date_from"
KEY_DATE_TO = "date_to"

KEY_USE_SSH = "use_ssh"
KEY_SSH = "ssh"
KEY_SSH_PRIVATE_KEY = "#private_key"
KEY_SSH_PRIVATE_KEY_PASSWORD = "#private_key_password"
KEY_SSH_USERNAME = "username"
KEY_SSH_TUNNEL_HOST = "tunnel_host"
KEY_SSH_REMOTE_ADDRESS = "remote_address"
KEY_SSH_REMOTE_PORT = "remote_port"

KEY_STATE_PREVIOUS_COLUMNS = "previous_columns"
KEY_STATE_LAST_RUN = "last_run"

REQUIRED_PARAMETERS = [KEY_USERNAME, KEY_PASSWORD, KEY_LOADING_OPTIONS, KEY_DATA_OBJECT, KEY_SERVICE_NAME]
REQUIRED_IMAGE_PARS = []

LOCAL_BIND_ADDRESS = "localhost"
LOCAL_BIND_PORT = 9800


class Component(ComponentBase):

    def __init__(self):
        self.ssh_server = None
        self.client = None
        self.state = None
        self.date_to = None
        self.date_from = None
        self.table_handlers = {}

        super().__init__()
        self.new_state = self._init_new_state()

    def run(self):
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        self.state = self.get_state_file()

        self.date_from = self._get_date_from()
        self.date_to = self._get_date_to()
        self._validate_fetching_mode()

        params = self.configuration.parameters

        self._init_client()

        if params.get(KEY_USE_SSH):
            self._create_and_start_ssh_tunnel()

        object_to_fetch = params.get(KEY_DATA_OBJECT)
        fields_to_fetch = self._get_fields_to_fetch()

        self._log_what_will_be_fetched(object_to_fetch)

        object_metadata = self._get_object_metadata(object_to_fetch)

        self._init_table_handlers(object_metadata, fields_to_fetch)
        self._fetch_and_write_data(object_metadata, fields_to_fetch)
        self._close_table_handlers()

        self.write_state_file(self.new_state)

    def _init_client(self) -> None:
        params = self.configuration.parameters
        username = params.get(KEY_USERNAME)
        password = params.get(KEY_PASSWORD)
        k2_address = self._get_k2_address()
        service_name = params.get(KEY_SERVICE_NAME)

        self.client = K2Client(username, password, k2_address, service_name)

    def _init_new_state(self) -> dict:
        statefile = self.get_state_file()
        previous_columns_data = statefile.get(KEY_STATE_PREVIOUS_COLUMNS) if statefile else None
        if previous_columns_data is None:
            previous_columns_data = {}

        return {"last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                          KEY_STATE_PREVIOUS_COLUMNS: previous_columns_data}

    def _fetching_is_incremental(self) -> bool:
        params = self.configuration.parameters
        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        load_type = loading_options.get(KEY_LOAD_TYPE)
        return load_type == "Incremental load"

    def _get_incremental_field(self) -> Optional[str]:
        params = self.configuration.parameters
        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        incremental = self._fetching_is_incremental()
        return loading_options.get(KEY_INCREMENTAL_FIELD) if incremental else None

    def _get_date_from(self) -> Optional[str]:
        params = self.configuration.parameters
        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        incremental = self._fetching_is_incremental()
        last_run = self.state.get(KEY_STATE_LAST_RUN)
        return self._get_parsed_date(loading_options.get(KEY_DATE_FROM), last_run) if incremental else None

    def _get_date_to(self) -> Optional[str]:
        params = self.configuration.parameters
        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        incremental = self._fetching_is_incremental()
        return self._get_parsed_date(loading_options.get(KEY_DATE_TO), "now") if incremental else None

    def _get_fetching_conditions(self) -> str:
        params = self.configuration.parameters
        incremental_field = self._get_incremental_field()
        conditions = params.get(KEY_CONDITIONS)
        return self._update_conditions_with_incremental_options(conditions, incremental_field)

    def _get_fields_to_fetch(self) -> str:
        params = self.configuration.parameters
        if fields := params.get(KEY_FIELDS):
            fields = fields.replace(" ", "")
        return fields

    def _get_k2_address(self) -> str:
        params = self.configuration.parameters
        if params.get(KEY_USE_SSH):
            return f"http://{LOCAL_BIND_ADDRESS}:{LOCAL_BIND_PORT}"
        return params.get(KEY_SOURCE_URL)

    def _fetch_and_write_data(self, object_metadata: K2ObjectMetadata, fields_to_fetch: str) -> None:
        """
        Paginates through all data which needs to be fetched based on conditions specified in the config and
        parses and saves all data to their respective csv tables in the data directory.

        Args:
            object_metadata: Metadata of the object that will be fetched
            fields_to_fetch: String of comma separated fields that will be fetched

        """
        child_object_foreign_keys = self._get_child_foreign_keys(object_metadata, fields_to_fetch)
        object_name = object_metadata.class_name
        conditions = self._get_fetching_conditions()
        try:
            for i, page_data in enumerate(self.client.get_object_data(object_name, fields_to_fetch, conditions)):
                if i % 20 == 0:
                    logging.info(f"Fetching page {i}")
                parsed_data = self._parse_object_data(page_data, object_name, child_object_foreign_keys)
                for parsed_data_name in parsed_data:
                    if parsed_data_name in self.table_handlers:
                        self.table_handlers[parsed_data_name].writer.writerows(parsed_data[parsed_data_name])
        except K2ClientException as k2_exc:
            raise UserException(k2_exc) from k2_exc
        except requests.exceptions.HTTPError as http_exc:
            raise UserException(http_exc) from http_exc
        except requests.exceptions.ConnectionError as http_exc:
            raise UserException("Could not connect to K2 API") from http_exc

    @staticmethod
    def _parse_object_data(k2_data: List[Dict], data_object, child_objects: Dict) -> Dict:
        parser = K2DataParser(child_object_parent_primary_keys=child_objects)
        return parser.parse_data(k2_data, data_object)

    def _get_object_metadata(self, object_class_name: str) -> K2ObjectMetadata:
        try:
            return K2ObjectMetadata(self.client.get_object_meta(object_class_name))
        except K2ClientException as k2exc:
            raise UserException("Authorization is incorrect, please validate the username, "
                                "password, service, and data object for K2") from k2exc
        except requests.exceptions.ConnectionError as e:
            raise UserException("Failed to connect to K2 Address and port, please validate if it is correct") from e

    @staticmethod
    def _generate_table_metadata(metadata: K2ObjectMetadata, table_columns: List[str]) -> TableMetadata:
        """
        Converts the metadata of a K2 object to Keboola Table metadata

        Args:
            metadata: metadata of a K2 object
            table_columns: columns in the resulting Keboola table

        Returns:
            Table metadata generated from K2 Object metadata

        """
        tm = TableMetadata()
        if metadata.caption:
            tm.add_table_description(metadata.caption)
        column_descriptions = {}
        for column in metadata.field_definitions:
            if column.get(K2_OBJECT_FIELD_NAME_KEY) in table_columns:
                column_descriptions[column.get(K2_OBJECT_FIELD_NAME_KEY)] = column.get("Description", "")
        tm.add_column_descriptions(column_descriptions)
        return tm

    @staticmethod
    def _get_parsed_date(date_input: Optional[str], last_run: Optional[str]) -> Optional[str]:
        if not date_input:
            parsed_date = None
        elif date_input.lower() in ["last", "last run"] and last_run:
            parsed_date = dateparser.parse(last_run)
        elif date_input.lower() in ["now", "today"]:
            parsed_date = datetime.now()
        elif date_input.lower() in ["last", "last run"] and not last_run:
            parsed_date = dateparser.parse("1990-01-01")
        else:
            try:
                parsed_date = dateparser.parse(date_input).date()
            except (AttributeError, TypeError) as err:
                raise UserException(f"Cannot parse date input {date_input}") from err
        if parsed_date:
            parsed_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
        return parsed_date

    def _update_conditions_with_incremental_options(self, conditions: str, incremental_field: str) -> str:
        """
        Updates fetching conditions to contain the incremental filter so only those data are fetched.

        Args:
            conditions: string of conditions with the format defined in
                        https://help.k2.cz/k2ori/02/en/10023272.htm#o106273
            incremental_field: The name of the field that is being used for incremental fetching, e.g. Timestamp

        Returns: updated conditions string

        """
        if incremental_field and self.date_from and self.date_to:
            incremental_condition = f"{incremental_field};GE;{self.date_from},{incremental_field};LE;{self.date_to}"
            if conditions:
                conditions = f"{conditions},{incremental_condition}"
            else:
                conditions = incremental_condition
        return conditions

    def _create_and_start_ssh_tunnel(self) -> None:
        self._create_ssh_tunnel()
        try:
            self.ssh_server.start()
        except BaseSSHTunnelForwarderError as e:
            raise UserException(
                "Failed to establish SSH connection. Recheck all SSH configuration parameters") from e

    def _create_ssh_tunnel(self) -> None:
        params = self.configuration.parameters
        ssh = params.get(KEY_SSH)
        private_key = ssh.get(KEY_SSH_PRIVATE_KEY)
        private_key_password = ssh.get(KEY_SSH_PRIVATE_KEY_PASSWORD)
        try:
            private_key = get_private_key(private_key, private_key_password)
        except SomeSSHException as key_exc:
            raise UserException from key_exc
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

    @staticmethod
    def _add_parent_prefix_to_keys(parent_prefix: str, primary_keys: List[str]) -> List[str]:
        return [f"{parent_prefix}_{pk}" for pk in primary_keys]

    def _find_child_objects(self, k2_object_class_name: str, k2_object_fields: List[str]) -> List[Dict]:
        """
        Passes through a list of fields of a k2 object and determines which ones are child objects. Child objects are
        then returned in a list of dictionaries.

        Args:
            k2_object_class_name: name of k2 object data is being fetched for
            k2_object_fields: list of fields that are being fetched

        Returns:
            A list of dictionaries containing child objects Class names, Fieldnames, and the corresponding class name
            and primary keys of the parent object of the child

        """
        object_metadata = self._get_object_metadata(k2_object_class_name)
        all_child_objects = []
        for k2_object_field in k2_object_fields:
            if child_objects := self._find_child_object(object_metadata, k2_object_field):
                all_child_objects.extend(child_objects)
        return all_child_objects

    def _find_child_object(self, object_metadata: K2ObjectMetadata, k2_object_field: str, all_child_objects=None):
        """
        Recursively goes through a field name to find if it is a child object.
        A '.' signifies a parent child relationship so field name of ObjectA.ObjectB signifies that B is a child of A,
        and we return the metadata of the B object. Once the final child object is found for the field name it is
        returned. If the field is just a field of the object and not a child object, then None is returned.

        Args:
            object_metadata: the metadata of the parent object of the field
            k2_object_field: the name of the field which is used to determine whether it is a child object

        Returns:
            Metadata about a child, in the form of a dictionary containing the child's Class name, Field name,
            and the corresponding class name and primary keys of the parent object of the child

        """
        if not all_child_objects:
            all_child_objects = []
        if "." in k2_object_field:
            split_text = k2_object_field.split(".")
            main_field_name = split_text[0]
            main_class_name = object_metadata.get_child_class_name_from_field_name(main_field_name)

            if main_class_name:
                child_object_metadata = self._get_object_metadata(main_class_name)
                all_child_objects.append(object_metadata.get_child_metadata(main_field_name))
                childs_children = ".".join(split_text[1:])
                self._find_child_object(child_object_metadata, childs_children, all_child_objects)
        else:
            child_class_name = object_metadata.get_child_class_name_from_field_name(k2_object_field)
            if child_class_name:
                all_child_objects.append(object_metadata.get_child_metadata(k2_object_field))
        return all_child_objects

    def _get_child_foreign_keys(self, parent_object_metadata: K2ObjectMetadata, fields: str) -> Dict:
        """
        Passes through all child objects and find the primary keys of each child's parents, this data
        is then used for parsing the data, as the parent primary keys should be saved with the child data so they can
        be linked.

        Args:
            parent_object_metadata: metadata of the parent object of the child
            fields: str containing fields to be fetched

        Returns: A dictionary with the key as the string f"{parent class name}_{child field name}" and the value as the
        primary keys of the parent object of the child

        """
        parsed_fields = comma_separated_values_to_list(fields)
        child_objects = self._find_child_objects(parent_object_metadata.class_name, parsed_fields)
        child_foreign_keys = {}
        for child_object in child_objects:
            parent_metadata = self._get_object_metadata(child_object.get(K2_OBJECT_PARENT_CLASS_NAME_KEY))
            parents_primary_keys = parent_metadata.primary_key_names
            key_name = f"{parent_metadata.class_name}_{child_object.get(K2_OBJECT_FIELD_NAME_KEY)}"
            child_foreign_keys[key_name] = parents_primary_keys
        return child_foreign_keys

    def _validate_fetching_mode(self) -> None:
        incremental_load = self._fetching_is_incremental()
        incremental_field = self._get_incremental_field()
        if incremental_load and (not incremental_field or not self.date_from or not self.date_to):
            raise UserException("To run incremental load mode you need to specify the incremental field, "
                                "date from and date to")

    def _get_fields_from_previous_run(self, object_name: str) -> List[str]:
        return self.state.get(KEY_STATE_PREVIOUS_COLUMNS, {}).get(object_name) or []

    def _log_what_will_be_fetched(self, object_to_fetch) -> None:
        logging.info(f"Fetching object : {object_to_fetch}")
        if self._fetching_is_incremental():
            logging.info(f"Fetching data from {self.date_from} to {self.date_to}")

    def _init_table_handlers(self, object_metadata: K2ObjectMetadata, fields_to_fetch: str) -> None:
        """
         Initializes the main table handler and child table handlers (A Table handler is an object that holds the
         metadata of the object to be downloaded, the table writer, and the table definition).
         The main table handler is for the data that corresponds to the main object. The child table handlers are for
         all the child objects of the main table data.
         Each table handler is stored in the Table Handler dictionary variable in the component. It can be accessed by
         the name of the table handler, the main table handler name is the Class name of the object and the
         name of the child table handlers is the {Parent Class Name}_{Child Name}

        Args:
            object_metadata: Metadata of the object in K2 that will be fetched
            fields_to_fetch: Comma separated list of fields of the K2 object that should be fetched, if empty, all
                             fields will be fetched. It is possible to specify child objects in the fields
        """
        incremental_load = self._fetching_is_incremental()
        self._init_main_table_handler(object_metadata, incremental_load)
        self._init_child_table_handlers(object_metadata, fields_to_fetch, incremental_load)

    def _init_main_table_handler(self, object_metadata: K2ObjectMetadata, incremental_load: bool) -> None:
        object_name = object_metadata.class_name
        object_fields_from_previous_run = self._get_fields_from_previous_run(object_name)

        table_definition = self.create_out_table_definition(f"{object_name}.csv",
                                                            primary_key=object_metadata.primary_keys,
                                                            incremental=incremental_load)

        writer = ElasticDictWriter(table_definition.full_path, object_fields_from_previous_run)
        self.table_handlers[object_name] = TableHandler(table_definition=table_definition,
                                                        writer=writer,
                                                        object_metadata=object_metadata)

    def _init_child_table_handlers(self, parent_object_metadata: K2ObjectMetadata, fields: str,
                                   incremental: bool) -> None:
        parsed_fields = comma_separated_values_to_list(fields)
        child_objects = self._find_child_objects(parent_object_metadata.class_name, parsed_fields)

        for child_object in child_objects:
            self._init_child_table_handler(child_object, incremental)

    def _init_child_table_handler(self, child_object: Dict, incremental: bool) -> None:
        """
        Initializes a table handler for a specific child object.
        Initializes the table definition for the handler by creating a table name and finding the primary keys
        Initializes the table writer for the handler using the table definition and columns from the state (all columns
        from the previous run should be present in the ouptut table).

        Args:
            child_object: Metadata about a child, in the form of a dictionary containing the child's Class name,
                          Field name, and the corresponding class name and primary keys of the parent object of
                          the child
            incremental: boolean value indicating whether the table should be incrementally loaded into KBC storage

        """
        parent_class_name = child_object.get(K2_OBJECT_PARENT_CLASS_NAME_KEY)
        child_class_name = child_object.get(K2_OBJECT_CLASS_NAME_KEY)
        child_field_name = child_object.get(K2_OBJECT_FIELD_NAME_KEY)
        full_name = f"{parent_class_name}_{child_field_name}"
        table_name = f"{full_name}.csv"

        object_metadata = self._get_object_metadata(child_object.get(K2_OBJECT_CLASS_NAME_KEY))

        parent_keys_with_prefix = self._add_parent_prefix_to_keys(parent_class_name,
                                                                  child_object.get(K2_OBJECT_PARENT_PRIMARY_KEYS))
        child_primary_keys = object_metadata.primary_key_names
        child_primary_keys += parent_keys_with_prefix

        child_table_definition = self.create_out_table_definition(table_name,
                                                                  primary_key=child_primary_keys,
                                                                  incremental=incremental)

        child_prev_columns = self.state.get(KEY_STATE_PREVIOUS_COLUMNS, {}).get(child_class_name, [])
        writer = ElasticDictWriter(child_table_definition.full_path, child_prev_columns)

        self.table_handlers[full_name] = TableHandler(table_definition=child_table_definition,
                                                      object_metadata=object_metadata,
                                                      writer=writer)

    def _close_table_handlers(self) -> None:
        for table_handler in self.table_handlers:
            self._close_table_handler(self.table_handlers[table_handler])

    def _close_table_handler(self, table_handler: TableHandler) -> None:
        """
        Closes the table handler writer. Updates the table definition columns with the final columns of the writer.
        Adds table metadata to the table definition. Writes the manifest of the table. Updates new state with the
        columns of the table.

        Args:
            table_handler: the table handler to be closed

        """
        k2_object_name = table_handler.object_metadata.class_name

        table_handler.writer.close()

        final_fields = table_handler.writer.fieldnames
        table_handler.table_definition.columns = final_fields

        table_handler.table_definition.table_metadata = self._generate_table_metadata(
            metadata=table_handler.object_metadata,
            table_columns=final_fields)

        self.write_manifest(table_handler.table_definition)

        self.new_state[KEY_STATE_PREVIOUS_COLUMNS].update({f"{k2_object_name}": final_fields})


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
