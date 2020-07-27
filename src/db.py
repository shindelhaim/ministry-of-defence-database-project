import os, shelve
from typing import Any, Dict, List, Type
import db_api


class DBField(db_api.DBField):
    def __init__(self, name, type):
        self.name = name
        self.type = type


class SelectionCriteria(db_api.SelectionCriteria):
    field_name: str
    operator: str
    value: Any


class DBTable(db_api.DBTable):  
    def __init__(self, name, fields, key_field_name):
        super.__init__()
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.num_record = 0
        self.path_file = os.path.join('db_files', self.name + '.db')

        # create shelve file
        s = shelve.open(self.path_file)
        s.close()


    def count(self) -> int:
        return self.num_record


    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.key_field_name not in values.keys():
            raise KeyError("The key is missing")

        s = shelve.open(self.path_file)

        if self.key_field_name in s.keys():
            raise KeyError("The key must be unique")
 
        self.fields += list(set(values.keys()).difference(set(self.fields)))
        s[values[self.key_field_name]] = values
        s.close()
        

    def delete_record(self, key: Any) -> None:
        s = shelve.open(self.path_file)

        if key not in s.keys():
            raise KeyError("The key isn't exists")
 
        s.pop(key)
        s.close()
        

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError


    def get_record(self, key: Any) -> Dict[str, Any]:
        s = shelve.open(self.path_file)
        record = s.get(key, None)
        s.close()
        return record


    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        s = shelve.open(self.path_file, writeback=True)
        self.fields += list(set(values.keys()).difference(set(self.fields)))
        s[key].update(values)
        s.close()


    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError


    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


class DataBase(db_api.DataBase):
    def __init__(self):
        super.__init__()
        self.db_tables = {}
        self.num_tables = 0


    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        self.db_tables[table_name] = DBTable(table_name, fields, key_field_name)
        self.num_tables += 1
        return self.db_tables[table_name]


    def num_tables(self) -> int:
        return self.num_tables


    def get_table(self, table_name: str) -> DBTable:
        return self.db_tables.get(table_name, None)


    def delete_table(self, table_name: str) -> None:
        self.db_tables.pop(table_name, None)
        

    def get_tables_names(self) -> List[Any]:
        return self.db_tables.keys()


    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
