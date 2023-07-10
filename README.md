# copy-postgres-database
a python script to copy 1 database to another

## install dependencies
```
pip install psycopg2
```

## Example db config file:
``` JSON
{
    "host": "localhost",
    "port": "5432",
    "database": "test_db",
    "user": "test_db_user",
    "password": "test_db_password"
}
```

## Create DBConnection object

``` python
# parse json config file
config = read_json_config('./path/to/config.json')

# instantiate database connection wrapper
db = DBConnection(config)
```

__note that the `editable_environment` param has to be set to `True` in order to write to a given database__
 - this is a small feature to help prevent accidentally wiping the source database
 - __but please make sure that you are passing the correct variables__

``` python
# a connection to READ FROM dev db
db_source = DBConnection(read_json_config('./secrets/db_config_dev.json'))

# a connection to WRITE TO localhost db
db_dest = DBConnection(
    read_json_config('./secrets/db_config_localhost.json'), 
    editable_environment=True
)
```

## UsefulFunctions.py
the `lib/UsefulFunctions.py` file contains functions for duplicating a single table &/or entire schema

```python
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
    truncate_tables: bool = False
)
```


## Example Code
``` python
UsefulFunctions.duplicate_table(
    db_source=DBConnection(read_json_config('./secrets/db_config_dev.json')),
    db_dest=DBConnection(
        read_json_config('./secrets/db_config_localhost.json'), 
        editable_environment=True
    ),
    schema_name='public', 
    table_name='inventory_forecast_store'
    truncate_table=True
)
```



# TODO:

## sanitize data
replace sensitive information with randomly generated pseudo info
- need to create a dictionary identifying all `schema.table.column`(s) that need to be sanitized, and what type of data to replace those entries with

## nullify empty values?
some values have empty strings. change these to null?