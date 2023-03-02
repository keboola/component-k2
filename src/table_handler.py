class TableHandler:
    def __init__(self, table_definition, writer, object_metadata, parent_primary_keys=None, parent_name=None):
        self.table_definition = table_definition
        self.writer = writer
        self.object_metadata = object_metadata
        self.parent_primary_keys = parent_primary_keys
        self.parent_name = parent_name
