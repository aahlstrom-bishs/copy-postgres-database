import psycopg2
from urllib.parse import urlparse
import json
from typing import List, Tuple

class DBConnection:
    def __init__(self, _config: dict, editable_environment: bool = False):
        self.config = _config
        self.connection = psycopg2.connect(**_config)
        self.cursor = self.connection.cursor()
        self.connection.autocommit = False
        self.editable_environment = editable_environment

    def fetch_query(self, _sql_query_string: str, _default=None):
        self.cursor.execute(_sql_query_string)
        try:
            return self.cursor.fetchall()
        except Exception as e:
            print(f"an exception occurred when fetching the following query \n'{_sql_query_string}'\n-----\n ERROR : {str(e)}")
        return _default
    
    def execute(self, _sql_command_string: str):
        self.cursor.execute(_sql_command_string)
        
    def commit_transaction(self):
        self.assert_editable_environment()
        self.connection.commit()

    def rollback_transaction(self):
        self.connection.rollback()
    
    def get_host(self):
        return self.config['host']
    
    def get_database(self):
        return self.config['database']
    
    def assert_environment(self, _database_name: str, _message: str):
        if self.get_database() != _database_name:
            raise Exception(_message)

    def assert_editable_environment(self):
        if (not self.editable_environment):
            raise Exception("This function can only be called in an editable environment.")

    def assert_test_environment(self, _message: str):
        if self.get_host() != 'localhost':
            raise Exception(_message)


    def close(self):
        try:
            self.rollback_transaction()
            self.cursor.close()
            self.connection.close()
            print(f"closing DB connection : {self.config['host']}")
        except Exception as e:
            message = str(e)
            if message == 'connection already closed':
                print(f"{message} : {self.config['host']}")
            else:
                raise e

    def __del__(self):
        self.close()


    # db script logic functions


    def check_if_schema_exists(self, _schema_name: str):
        self.cursor.execute(f"""
            SELECT EXISTS(
                SELECT 1 
                FROM information_schema.schemata 
                WHERE schema_name = '{_schema_name}'
            )
        """)
        exists = self.cursor.fetchone()[0]
        return exists

    def check_if_table_exists(self, _schema_name: str, _table_name: str):
        self.cursor.execute(f"""
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.tables 
                WHERE 
                    table_schema = '{_schema_name}' 
                    AND table_name = '{_table_name}'
            )
        """)
        exists = self.cursor.fetchone()[0]
        return exists


    # commit transaction manually after this function is called.
    def create_schema_if_not_exists(self, _schema_name: str):
        self.assert_editable_environment()
        try:
            self.execute(f'CREATE SCHEMA IF NOT EXISTS {_schema_name};')
            # commit transaction manually after this function is called.
            print(f"Schema {_schema_name} created successfully.")
            return True
        except Exception as e:
            print(f"Failed to create schema '{_schema_name}' : \nError Message = '{str(e).strip()}'")
        self.rollback_transaction()
        return False
    
    # commit transaction manually after this function is called.
    def wipe_schema(self, _schema_name: str):
        self.assert_editable_environment()
        try:
            self.execute(f'DROP SCHEMA IF EXISTS {_schema_name};')
            # commit transaction manually after this function is called.
            print("schema wiped successfully")
            return True
        except Exception as e:
            print(f"Failed to wipe schema '{_schema_name}' : \nError Message = '{str(e).strip()}'")
        self.rollback_transaction()
        return False

    

    # this function is intended to accept data requested straight from the source databse.
    # commit transaction manually after this function is called.
    def insert_record(self, _schema_name: str, _table_name: str, _column_names: list[str], _row_values: list[any]):
        self.assert_editable_environment()
        row_values_string_template = ','.join(['%s'] * len(_row_values))
        sql_command_insert_into = f"""
            INSERT INTO {_schema_name}.{_table_name} 
            ({",".join(_column_names)}) 
            VALUES ({row_values_string_template})
        ;"""
        # use cursor instead of DBConnection.execute so that _row_values can be passed as a parameter.
        self.cursor.execute(sql_command_insert_into, _row_values)
        # commit transaction manually after this function is called.



    def create_empty_duplicate_table(self, _db_source: "DBConnection", _schema_name: str, _table_name: str, _truncate: bool = False):
        self.assert_editable_environment()
        try:
            if not self.check_if_schema_exists(_schema_name):
                self.create_schema_if_not_exists(_schema_name); # permission denied for dev DB
            if not self.check_if_table_exists(_schema_name, _table_name):
                # create table with same columns as source table
                column_query = f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema='{_schema_name}'
                    AND table_name='{_table_name}'
                ;"""
                column_names = _db_source.fetch_query(column_query)
                if column_names == None:
                    raise Exception(f"query failed : {column_query}")
                
                column_definitions = ', '.join([f'"{col[0]}" {col[1]}' for col in column_names])
                self.execute(f'CREATE TABLE {_schema_name}.{_table_name} ({column_definitions});')
            if (_truncate):
                # erase all data in the table
                self.execute(f'TRUNCATE TABLE {_schema_name}.{_table_name};')
            print(f"Table {_schema_name}.{_table_name} created successfully.")
            # commit transaction manually after this function is called.
            return True
        except Exception as e:
            print(f"Failed to create table '{_schema_name}.{_table_name}' : \n    !! Error Message = '{str(e).strip()}'")
        self.rollback_transaction()
        return False


    def create_and_populate_duplicate_table(self, _db_source: "DBConnection", _schema_name: str, _table_name: str, _truncate: bool = False):
        self.assert_editable_environment()
        try:    
            if not self.create_empty_duplicate_table(_db_source, _schema_name, _table_name, _truncate):
                raise Exception("failed to create table")
            
            rows = _db_source.fetch_query(f'SELECT * FROM {_schema_name}.{_table_name};')
            if rows == None:
                raise Exception(
                    f"  ! Error requesting rows from {_schema_name}.{_table_name} :\n" +
                    f"    !! table population failed for table = {_table_name}"
                )
            elif not bool(rows):
                # TODO : check table record count
                print(f"  ? no records to populate {_schema_name}.{_table_name}")
                return False

            column_names = []
            for desc in _db_source.cursor.description:
                column_names.append(f'"{desc[0]}"')
            
            print(f"  > table '{_schema_name}.{_table_name}'")
            print(f"  > col ({len(column_names)}) : {column_names}")
            print(f"  > row ({len(rows[0]) }) : {rows[0]}")
            try:
                for row_values in rows:
                    self.insert_record(_schema_name, _table_name, column_names, row_values)
                self.commit_transaction()
                print(f"table population completed successfully : table = {_table_name}")
                return True
            except Exception as e:
                self.rollback_transaction()
                print(
                    f"  ! An error occurred while populating table rows for : '{_schema_name}.{_table_name}' :\n" +
                    f"    !! {str(e)}"
                )
        except Exception as e:
            self.rollback_transaction()
            print(
                f"  ! Failed to Populate table '{_schema_name}.{_table_name}' :\n" +
                f"    !! '{str(e).strip()}'"
            )
            raise e  # unknown failure = catastrophic failure
        return False

    # Copy tables and records from source to destination
    def create_and_populate_duplicate_schema(
        self, 
        db_source: "DBConnection", 
        _schema_name: str, 
        _truncate_tables: bool = False,
        filter_keywords: list = []        
    ):
        self.assert_editable_environment()
        try:
            if not (
                # self.wipe_schema(_schema_namae) and
                self.create_schema_if_not_exists(_schema_name)
            ):
                self.rollback_transaction()
                return False
            
            self.commit_transaction()
            tables = db_source.fetch_query(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='{_schema_name}' AND table_type='BASE TABLE';
            """)
            # fix me : this is just temporary to avoid losing time on tables that are already populated
            dest_tables = self.fetch_query(f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='{_schema_name}' AND table_type='BASE TABLE';
            """)
            dest_tables = [table[0] for table in dest_tables]
            print('existing tables: ', dest_tables)

            for table in tables:
                print("")
                table_name = table[0]
                if table_name in dest_tables: # fix me : this is just temporary to avoid losing time on tables that are already populated
                    print(f"  ? table '{_schema_name}.{table_name}' already exists")
                    if any([keyword in table_name for keyword in filter_keywords]):
                        print(f"  !! dropping table '{_schema_name}.{table_name}'")
                        self.execute(f"DROP TABLE {_schema_name}.{table_name};")
                elif any([keyword in table_name for keyword in filter_keywords]):
                    print(f"  ? skipping table '{_schema_name}.{table_name}'")
                else:
                    self.create_and_populate_duplicate_table(db_source, _schema_name, table_name, _truncate_tables)
            print(f"Database population completed successfully. : schema = {_schema_name}")
            return True
        except Exception as e:
            print(f'an error was received while populating tables for schema = {_schema_name} :\n    !! {str(e)}')
        self.rollback_transaction()
        return False




    # ---------------------------------------------
    # Reading DB
    # ---------------------------------------------



    
    def to_dict(self, records: List[Tuple], list_column_names: List[str] = None):
        col_names = list_column_names or [desc[0] for desc in self.cursor.description]
        data = {}
        for name in col_names:
            data[name] = []
        for record in records:
            k = 0
            while k < len(record):
                data[col_names[k]].append(str(record[k]))
                k += 1
        return data
    
    def get_list_db_schemas(self):
        results = self.fetch_query("""
            SELECT nspname 
            FROM pg_namespace 
            WHERE nspname NOT LIKE 'pg_%' 
            AND nspname != 'information_schema'
        """)
        names = [row[0] for row in results]
        return names

    def get_list_schema_tables(self, _schema_name: str):
        results = self.fetch_query(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='{_schema_name}' AND table_type='BASE TABLE';
        """)
        names = [row[0] for row in results]
        return names

    def get_dict_table_columns(self, _schema_name: str, _table_name: str):
        try:
            _sql_query_string = f"""
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_schema='{_schema_name}'
                AND table_name='{_table_name}'
            ;"""
            result = self.fetch_query(_sql_query_string)
            cols = {}
            for col in result:
                cols[col[0]] = col[2]
            return cols
        except Exception as e:
            print(f"Failed to query table '{_schema_name}.{_table_name}' : \n    !! Error Message = '{str(e).strip()}'")
        self.rollback_transaction()
        return None

    # Function to get all primary keys for a table
    def get_list_primary_keys(self, _schema_name: str, _table_name: str):
        try:
            sql_query = f"""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a 
                    ON a.attrelid = i.indrelid 
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = '{_schema_name}.{_table_name}'::regclass 
                AND i.indisprimary
            """
            result = self.fetch_query(sql_query)
            primary_keys = [row[0] for row in result]
            return primary_keys
        except Exception as e:
            print(f"  ! Failed to query primary keys for '{_schema_name}.{_table_name}' : \n" +
                  f"    !! Error Message = '{str(e).strip()}'")
        self.rollback_transaction()
        return None
    
    # Function to get all foreign keys for a table
    def get_list_formalized_foreign_keys(self, _schema_name: str, _table_name: str):
        try:
            sql_query = f"""
                SELECT 
                    conname
                    , conrelid::regclass AS table_from
                    , a1.attname AS column_from
                    , confrelid::regclass AS table_to
                    , a2.attname AS column_to
                FROM pg_constraint
                JOIN pg_attribute a1 
                    ON a1.attnum = ANY(conkey) 
                    AND a1.attrelid = conrelid
                JOIN pg_attribute a2 
                    ON a2.attnum = ANY(confkey) 
                    AND a2.attrelid = confrelid
                WHERE conrelid = '{_schema_name}.{_table_name}'::regclass 
                AND contype = 'f'
            """
            result = self.fetch_query(sql_query)
            foreign_keys = []
            for row in result:
                constraint_name, table_from, column_from, table_to, column_to = row
                foreign_keys.append({
                    'constraint_name': constraint_name,
                    'table_from': table_from,
                    'column_from': column_from,
                    'table_to': table_to,
                    'column_to': column_to
                })
            return foreign_keys
        except Exception as e:
            print(f"  ! Failed to query formalized foreign keys for '{_schema_name}.{_table_name}' : \n" +
                  f"    !! Error Message = '{str(e).strip()}'")
        self.rollback_transaction()
        return None

    def get_list_unformalized_foreign_keys(self, _schema_name: str, _table_name: str, fk_identifier: str = '_'):
        columns = self.get_dict_table_columns(_schema_name, _table_name)
        col_names = columns.keys()
        fks = [key for key in col_names if fk_identifier in key]
        return fks


    
