from DBConnection import DBConnection
import time;
from typing import List


def flatten_list(lst):
    return [
        item 
        for sublist 
        in lst 
            for item 
            in (
                flatten_list(sublist) 
                if isinstance(sublist, list) 
                else [sublist]
            )
    ]


def prompt(message: str):
    print('awaiting confirmation: ')
    print(f"  > see the prompt above to confirm that you want to: {message}")
    time.sleep(0.5) # wait for the system to print the message
    return input(f"type yes to confirm: \n'{message}'") == "yes"

"""


copies a single table & contained records from the source database into the destination database
db_source   = the database to copy from
db_dest     = the database to write to
schema_name = the name of the schema to copy
table_name  = the name of the table to copy 
truncate_table = if true, the destination table will be truncated before copying

-- if the table does not exist in the destination database, it will be created
-- otherwise, the table will not be edited, and the records will be appended to the existing table
-- this is of note because if the table already exists, the table structure must already match the source table
"""
def duplicate_table(
    db_source: DBConnection, 
    db_dest: DBConnection, # must be an editable environment
    schema_name: str, 
    table_name: str,
    truncate_table: bool = False
):
    print('source:      ', db_source.get_database(), ' : ', db_source.get_host())
    print('destination: ', db_dest.get_database(), ' : ', db_dest.get_host())
    print('')
    print('schema:      ', schema_name)
    print('table:       ',  table_name)
    print('')

    _user_confirmation = prompt(f"{'TRUNCATE & ' if truncate_table else ''}DUPLICATE the table")

    if (not _user_confirmation):
        print("\nexecution aborted...\n")
        return
    
    print(f"\n{'truncating destination table & ' if truncate_table else ''}duplicating records: ", table_name)
    db_dest.create_and_populate_duplicate_table(
        db_source, 
        schema_name, 
        table_name,
        truncate_table
    )




"""
copies a single schema, including all tables & records contained within the schema 
db_source   = the database to copy from
db_dest     = the database to write to
schema_name = the name of the schema to copy
truncate_tables = if true, all tables in the destination schema will be truncated before copying

-- if the schema &/or tables do not exist in the destination database, they will be created
-- otherwise, existing tables will not be edited, and the records will be appended to the existing tables
-- this is of note because if a table already exists, the table structure must already match the source table
"""
def duplicate_schema(
    db_source: DBConnection, 
    db_dest: DBConnection, # must be an editable environment
    schema_name: str,
    truncate_tables: bool = False,
    filter_keywords: list = []
):
    print('source:      ', db_source.get_database(), ' : ', db_source.get_host())
    print('destination: ', db_dest.get_database(), ' : ', db_dest.get_host())
    print('awaiting confirmation: ')
    _user_confirmation = prompt(f"{'TRUNCATE destination tables & ' if truncate_tables else ''}DUPLICATE the schema")
    
    if (not _user_confirmation):
        print("\nexecution aborted...\n")
        return
    
    print(f"\n{'truncating existing destination tables & ' if truncate_tables else ''}duplicating schema records: ", schema_name)
          
    if (
        db_dest.create_and_populate_duplicate_schema(
            db_source, 
            schema_name,
            truncate_tables,
            filter_keywords
        )
    ):
        print("Database population completed successfully.")
    else:
        print("Database population failed")

