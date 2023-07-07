from DBConnection import DBConnection

"""
copies a single table & contained records from the source database into the destination database
db_source   = the database to copy from
db_dest     = the database to write to
schema_name = the name of the schema to copy
table_name  = the name of the table to copy 

-- if a matching table EXISTS in the destination database, IT WILL BE TRUNCATED
-- if the table DOES NOT EXIST in the destination database, IT WILL BE CREATED
"""
def duplicate_table(
    _db_source: DBConnection, 
    _db_dest: DBConnection, # must be an editable environment
    _schema_name: str, 
    _table_name: str
):
    print('source:      ', _db_source.get_database(), ' : ', _db_source.get_host())
    print('destination: ', _db_dest.get_database(), ' : ', _db_dest.get_host())
    print('')
    print('schema:      ', _schema_name)
    print('table:       ', _table_name)
    print('')
    print('awaiting confirmation: ')
    print('  > see the prompt above to confirm that you want to wipe and duplicate the table...')
    user_confirmation = input("type yes to confirm that you want to wipe and duplicate the table: ")

    if (user_confirmation != "yes"):
        print("\nexecution aborted...\n")
        return
    
    print("\nwiping and duplicating table: ", _table_name)
    _db_dest.create_and_populate_duplicate_table(
        _db_source, 
        _schema_name, 
        _table_name
    )




"""
copies a single schema, including all tables & records contained within the schema 
db_source   = the database to copy from
db_dest     = the database to write to
schema_name = the name of the schema to copy

-- if the schema DOES NOT EXIST in the destination database, IT WILL BE CREATED
-- if a matching table EXISTS in the destination database, IT WILL BE TRUNCATED
"""
def wipe_and_duplicate_schema(
    _db_source: DBConnection, 
    _db_dest: DBConnection, # must be an editable environment
    _schema_name: str, 
):
    print('source:      ', _db_source.get_database(), ' : ', _db_source.get_host)
    print('destination: ', _db_dest.get_database(), ' : ', _db_dest.get_host())
    print('awaiting confirmation: ')
    print('  > see the prompt above to confirm that you want to wipe and duplicate the schema...')
    user_confirmation = input("type yes to confirm that you want to wipe and duplicate the schema: ")
    
    if (user_confirmation != "yes"):
        print("\nexecution aborted...\n")
        return
    
    print("\nwiping and duplicating schema: ", _schema_name)
          
    if (
        _db_dest.wipe_create_and_populate_duplicate_schema(
            _db_source, 
            _schema_name
        )
    ):
        print("Database population completed successfully.")
    else:
        print("Database population failed")