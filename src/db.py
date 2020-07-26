from db_api import DB, DBTable, DBField, SelectionCriteria
from typing import Any, Dict, List, Type


class DataBaseTable(DBTable):  
    def __init__(self, name, fields, key_field_name):
        super.__init__()
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.num_record = 0

        # יצירת קובץ שלב


    def count(self) -> int:
        raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError



class DataBase(DB):
    def __init__(self):
        super.__init__()
        self.db_tables = {}
        self.num_tables = 0

    
    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        self.db_tables[table_name] = DataBaseTable(table_name, fields, key_field_name)
        self.num_tables += 1
        return self.db_tables[table_name]


    def num_tables(self) -> int:
        raise NotImplementedError

    def get_table(self, table_name: str) -> DBTable:
        raise NotImplementedError

    def delete_table(self, table_name: str) -> None:
        raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
        raise NotImplementedError

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
