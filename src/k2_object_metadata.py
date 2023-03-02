from typing import Optional, List, Dict

K2_OBJECT_CLASS_NAME_KEY = "ClassName"
K2_OBJECT_FIELD_NAME_KEY = "FieldName"
K2_OBJECT_PARENT_CLASS_NAME_KEY = "ParentClassName"
K2_OBJECT_PARENT_PRIMARY_KEYS = "ParentPrimaryKeys"


class K2ObjectMetadata:
    def __init__(self, metadata: Dict):
        self.metadata = metadata

    @property
    def class_name(self) -> str:
        return self.metadata.get(K2_OBJECT_CLASS_NAME_KEY)

    @property
    def field_definitions(self) -> List:
        return self.metadata.get("FieldList", {})

    @property
    def caption(self) -> str:
        return self.metadata.get("Caption")

    @property
    def primary_key_names(self) -> List:
        return [primary_key.get(K2_OBJECT_FIELD_NAME_KEY) for primary_key in self.metadata.get("PrimaryKeyFieldList")]

    @property
    def field_list(self) -> List:
        return list(self.metadata.get("FieldList"))

    @property
    def child_list(self) -> List:
        return self.metadata.get("ChildList", [])

    @property
    def primary_keys(self) -> List[str]:
        primary_keys = []
        if primary_keys_list := self.metadata.get("PrimaryKeyFieldList"):
            primary_keys = [primary_key.get(K2_OBJECT_FIELD_NAME_KEY) for primary_key in primary_keys_list]
        return primary_keys

    def get_child_class_name_from_field_name(self, child_field_name: str) -> str:
        for child in self.child_list:
            if child_field_name == child.get(K2_OBJECT_FIELD_NAME_KEY):
                return child.get("ChildClassName")

    def get_child_metadata(self, child_field_name: str) -> Optional[Dict]:
        for child in self.child_list:
            if child_field_name == child.get(K2_OBJECT_FIELD_NAME_KEY):
                return {K2_OBJECT_CLASS_NAME_KEY: child.get("ChildClassName"),
                        K2_OBJECT_FIELD_NAME_KEY: child.get(K2_OBJECT_FIELD_NAME_KEY),
                        K2_OBJECT_PARENT_CLASS_NAME_KEY: self.metadata.get(K2_OBJECT_CLASS_NAME_KEY),
                        K2_OBJECT_PARENT_PRIMARY_KEYS: self.primary_keys}
