import os, shelve, csv
from typing import Any, Dict, List, Type
import db_api
import datetime as dt


class DBField(db_api.DBField):
    def __init__(self, name, type):
        self.name = name
        self.type = type


class SelectionCriteria(db_api.SelectionCriteria):
    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


class DBTable(db_api.DBTable):  
    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.path_file = os.path.join('db_files', self.name + '.db')
        self.indexes = []

        # create shelve file
        s = shelve.open(self.path_file)
        self.num_record = len(s.keys())
        s.close()

    
    def get_names_fields(self):
        return [field.name for field in self.fields]


    def get_path_index_file(self, field_name):
        return os.path.join('db_files', 'index_' + field_name + '_' + self.name + '.db')


    def count(self) -> int:
        return self.num_record


    def update_records_in_every_indexes(self, old_record, new_record):
        if len(old_record) != 0 and (old_record[self.key_field_name] != new_record[self.key_field_name]):
            raise ValueError("The key field couldn't be updated")

        for field_name in new_record.keys():
            if field_name not in self.indexes:
                continue

            if field_name not in old_record.keys():
                index_file = shelve.open(self.get_path_index_file(field_name), writeback=True)

                if str(new_record[field_name]) in index_file.keys():
                    index_file[str(new_record[field_name])] += [new_record[self.key_field_name]]

                else:
                    index_file[str(new_record[field_name])] = [new_record[self.key_field_name]]

                index_file.close()

            else:
                if new_record[field_name] == old_record[field_name]:
                    continue
                
                index_file = shelve.open(self.get_path_index_file(field_name), writeback=True)
                index_file[str(old_record[field_name])].remove(old_record[self.key_field_name])

                if index_file[str(old_record[field_name])] == []:
                    index_file.pop(str(old_record[field_name]))

                if str(new_record[field_name]) in index_file.keys():
                    index_file[str(new_record[field_name])] += [new_record[self.key_field_name]]

                else:
                    index_file[str(new_record[field_name])] = [new_record[self.key_field_name]]

                index_file.close()


    def delete_records_from_every_indexes(self, records: List[Dict[str, Any]]):

        for index in self.indexes:
            is_index_file_open = False
            
            for record in records:
                if index in record.keys():
                    
                    if not is_index_file_open:
                        index_file = shelve.open(self.get_path_index_file(index), writeback=True)
                        is_index_file_open = True
                    
                    index_file[str(record[index])].remove(record[str(self.key_field_name)])
                    
                    if index_file[str(record[index])] == []:
                        index_file.pop(str(record[index]))
            
            if is_index_file_open:
                index_file.close()

    
    def insert_record(self, values: Dict[str, Any]) -> None:
        if self.key_field_name not in values.keys():
            raise ValueError("The key is missing")

        s = shelve.open(self.path_file)

        if str(values[self.key_field_name]) in s.keys():
            s.close()
            raise ValueError("The key must be unique")
 
        self.fields += [ DBField(item, Any) for item in values.keys() if item not in self.get_names_fields()]    
        s[str(values[self.key_field_name])] = values
        self.update_records_in_every_indexes({}, values)   
        self.num_record += 1
        s.close()
        

    def delete_record(self, key: Any) -> None:
        s = shelve.open(self.path_file, writeback=True)

        if str(key) not in s.keys():
            s.close()
            raise ValueError("The key doesn't exist")
        
        self.delete_records_from_every_indexes([s[str(key)]])
        s.pop(str(key))
        self.num_record -= 1
        s.close()


    def are_criterias_met(self, record: Dict[str, Any], criterias: List[SelectionCriteria]):
        for criteria in criterias:
            if criteria.field_name in record.keys():
                if criteria.operator == '=':
                    criteria.operator = "=="
                try:
                    is_criteria_met = eval(f'{record[criteria.field_name]} {criteria.operator} {criteria.value}')
                
                except NameError:

                    is_criteria_met = eval(f'str(record[criteria.field_name]) {criteria.operator} str(criteria.value)')
                
                if not is_criteria_met:
                    return False

            else:
                return False
        
        return True


    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        table_file = shelve.open(self.path_file, writeback=True)
        list_match_records = []

        # using hash index by key
        for item in criteria:
            if item.field_name == self.key_field_name and item.operator == '=':
                record = table_file.get(str(item.value), None)

                if record is None:
                    return

                if self.are_criterias_met(record, criteria):
                    table_file.pop(str(item.value))
                    self.delete_records_from_every_indexes([record])
                    self.num_record -= 1
                    table_file.close()
                
                return

        # using hash index if exist
        for item in criteria:
            if item.field_name in self.indexes and item.operator == '=':
                index_file = shelve.open(self.get_path_index_file(item.field_name), 'r')
                records_keys = table_file.get(str(item.value), None)

                if records_keys is None:
                    return
                
                for key in records_keys:
                    record = table_file[str(key)]

                    if self.are_criterias_met(record, criteria):
                        table_file.pop(str(key))
                        list_match_records += [record]
                        self.num_record -= 1

                table_file.close()
                index_file.close()
                self.delete_records_from_every_indexes(list_match_records)
                return
    
        for record in table_file.values():
            if self.are_criterias_met(record, criteria):
                table_file.pop(str(record[self.key_field_name]))
                list_match_records += [record]
                self.num_record -= 1
        
        table_file.close()
        self.delete_records_from_every_indexes(list_match_records)


    def get_record(self, key: Any) -> Dict[str, Any]:
        s = shelve.open(self.path_file)
        record = s.get(str(key), None)
        s.close()
        return record


    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        s = shelve.open(self.path_file, writeback=True)
        
        if str(key) not in s.keys():
            s.close()
            raise ValueError("The key doesn't exist")
        
        self.fields += [ DBField(item, Any) for item in values.keys() if item not in self.get_names_fields()]
        old_record = s[str(key)]
        s[str(key)].update(values)
        new_record = s[str(key)]
        self.update_records_in_every_indexes(old_record, new_record)
        s.close()


    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        table_file = shelve.open(self.path_file)
        list_match_records = []
        
        # using hash index by key
        for item in criteria:
            if item.field_name == self.key_field_name and item.operator == '=':
                record = table_file.get(str(item.value), None)

                if record is None:
                    return []

                if self.are_criterias_met(record, criteria):
                    table_file.close()
                    return [record]
                
                return []
        
        # using hash index if exist
        for item in criteria:
            if item.field_name in self.indexes and item.operator == '=':
                index_file = shelve.open(self.get_path_index_file(item.field_name))
                id_records = index_file.get(str(item.value), None)

                if id_records is None:
                    return []
                
                for id_record in id_records:
                    record = table_file[str(id_record)]
                    
                    if self.are_criterias_met(record, criteria):
                        list_match_records += [record]

                table_file.close()
                index_file.close()
                
                return list_match_records

        for record in table_file.values():
            if self.are_criterias_met(record, criteria):
                list_match_records += [record]
        
        table_file.close()
        return list_match_records


    def create_index(self, field_to_index: str) -> None:
        if field_to_index not in self.get_names_fields():
            raise ValueError("Field index doesn't exist in table's fields")

        if field_to_index in self.indexes or field_to_index == self.key_field_name:
            return

        index_file = shelve.open(self.get_path_index_file(field_to_index), writeback=True)
        table_file = shelve.open(self.path_file)

        for record in table_file.values():
            key_index = record.get(field_to_index, None)

            if key_index is None:
                continue

            key_index = str(key_index)
            
            if key_index in index_file.keys():
                index_file[key_index] += [record[self.key_field_name]]

            else:
                index_file[key_index] = [record[self.key_field_name]]

        table_file.close()
        index_file.close()

        # update the file database.csv
        with open('database.csv','r') as csv_file:
            csv_reader = csv.reader(csv_file)
            lines = []
            for row in csv_reader:
                if self.name == row[0]:
                    row[3] += [field_to_index]

                lines += [row]
            
        with open('database.csv','w',newline='') as csv_file:
            csv_writer = csv.writer(csv_file) 
            for line in lines:
                csv_writer.writerow(line)


class DataBase(db_api.DataBase):
    def __init__(self):
        self.db_tables = {}
        self.num_tables_in_DB = 0
        self.reload_from_disk()


    def get_db_field_obj(self, data_field: List):
        return DBField(data_field[0], eval(data_field[1]))
        

    def reload_from_disk(self):
        with open('database.csv', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)

            for row in csv_reader:
                table_name = row[0]
                list_fields = list(map(self.get_db_field_obj, eval(row[1])))
                key_field_name = row[2]
                self.db_tables[table_name] = DBTable(table_name, list_fields, key_field_name)
                self.db_tables[table_name].indexes = eval(row[3])
                self.num_tables_in_DB += 1

#####
    def get_data_field(self, field: DBField):
        
        if field.type == dt.datetime:
            type_as_str = 'dt.datetime'

        else:
            if isinstance(field.type, type):
                type_as_str = field.type.__name__
            else:
                type_as_str = field.type._name
        
        return [field.name, type_as_str]


    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        if table_name in self.db_tables.keys():
            raise ValueError("The table name exists in the database")

        if key_field_name not in [field.name for field in fields]:
            raise ValueError("The key doesn't exist in fields list")
        
        self.db_tables[table_name] = DBTable(table_name, fields, key_field_name)
        self.num_tables_in_DB += 1

        with open('database.csv', "a", newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            list_data_fields = list(map(self.get_data_field, fields))
            data_table = [table_name, list_data_fields, key_field_name, []]
            csv_writer.writerow(data_table)

        return self.db_tables[table_name]


    def num_tables(self) -> int:
        return self.num_tables_in_DB


    def get_table(self, table_name: str) -> DBTable:
        if table_name not in self.db_tables.keys():
            raise ValueError("The table name doesn't exist in the database")

        return self.db_tables.get(table_name, None)


    def delete_selve_file(self, table_name):
        s = (os.path.join('db_files', table_name + ".db.bak"))
        os.remove(s)
        s = (os.path.join('db_files', table_name + ".db.dat"))
        os.remove(s)
        s = (os.path.join('db_files', table_name + ".db.dir"))
        os.remove(s)


    def delete_table(self, table_name: str) -> None:
        if table_name not in self.db_tables.keys():
            raise ValueError("The table name doesn't exist in the database")
        
        self.num_tables_in_DB -= 1
        self.delete_selve_file(table_name)
        self.db_tables.pop(table_name)
        
        # remove the table from database.csv
        with open('database.csv','r') as csv_file:
            csv_reader = csv.reader(csv_file)
            lines = []
            for row in csv_reader:
                lines += [row]
            
        with open('database.csv','w',newline='') as csv_file:
            csv_writer = csv.writer(csv_file) 
            for line in lines:
                if line[0] != table_name:
                    csv_writer.writerow(line)
        

    def get_tables_names(self) -> List[Any]:
        return list(self.db_tables.keys())

##############
    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
