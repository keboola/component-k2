from typing import Union, List, Dict, Tuple, Optional
from enum import Enum, auto


class K2DataObject(Enum):
    BASE = auto()
    NESTED = auto()
    CHILD_TABLE = auto()


class K2DataParser:
    def __init__(self, child_objects):
        self.child_objects = child_objects

    def parse_data(self, json_data: List[Dict], main_table_name: str) -> Dict:
        final_data = {}
        for row in json_data:
            parsed_row = self._parse_row_to_tables(row, main_table_name)
            for key in parsed_row:
                if key not in final_data:
                    final_data[key] = []
                final_data[key].extend(parsed_row[key])
        return final_data

    def _parse_row_to_tables(self, data_object: Dict, main_table_name: str) -> Dict:
        table_data = {main_table_name: []}

        def init_table(key: str):
            if key not in table_data:
                table_data[key] = []

        def parse_field(customer_data: Dict, table_name: str, table_index: int = 0, parent_name: str = ""):
            for index, sub_data in enumerate(customer_data.get("FieldValues")):
                type_of_data = self.get_data_type(sub_data)
                if type_of_data == K2DataObject.BASE:
                    parse_object(table_name, sub_data, table_index, parent_name)
                elif type_of_data == K2DataObject.CHILD_TABLE:
                    child_table_name = sub_data.get("Name")
                    parent_pkeys = self.get_parent_pkeys_from_child(child_table_name)
                    data_to_send = sub_data.get("Value").get("Items")
                    p_key_name, p_key_val = self.get_primary_key(customer_data.get("FieldValues"), parent_pkeys)
                    p_key_name = f"{customer_data.get('DOClassName')}_{p_key_name}"
                    parse_child_table(data_to_send, child_table_name, p_key_name, p_key_val)
                elif type_of_data == K2DataObject.NESTED:
                    if parent_name:
                        new_parent_name = f"{parent_name}_{sub_data.get('Name')}"
                    else:
                        new_parent_name = sub_data.get('Name')
                    parse_field(sub_data.get("Value"), table_name, table_index, parent_name=new_parent_name)

        def parse_child_table(data: List[Dict], table_name: str, p_key_name: str, p_key_val: Union[str, int]):
            init_table(table_name)
            for index, datum in enumerate(data):
                parse_field(datum, table_name, index)
                table_data[table_name][index][p_key_name] = p_key_val

        def parse_object(table_name: str, data: Dict, table_index: int, parent_name: str = ''):
            if len(table_data[table_name]) < table_index + 1:
                table_data[table_name].append({})
            name = data.get("Name")
            if parent_name:
                name = f"{parent_name}_{data.get('Name')}"
            table_data[table_name][table_index][name] = data.get("Value")

        parse_field(data_object, main_table_name)
        return table_data

    @staticmethod
    def get_data_type(data_object: Dict) -> K2DataObject:
        data_type = K2DataObject.BASE
        if isinstance(data_object.get("Value"), dict):
            if data_object.get("Value").get("__type") == 'DataObjectWrapper:K2.Data':
                data_type = K2DataObject.NESTED
            elif data_object.get("Value").get("__type") == 'ChildDataObjectWrapper:K2.Data':
                data_type = K2DataObject.CHILD_TABLE
        return data_type

    @staticmethod
    def get_primary_key(data: List[Dict], pkey_names: List[str]) -> Tuple[Optional[str], Optional[Union[str, int]]]:
        for pkey_name in pkey_names:
            for datum in data:
                if datum.get("Name") == pkey_name:
                    return datum.get("Name"), datum.get("Value")
        return None, None

    def get_parent_pkeys_from_child(self, child_table_name):
        for child in self.child_objects:
            if child.get("field_name") == child_table_name:
                return child.get("parent_primary_keys")
        return ["RID", "ID", "Id"]
