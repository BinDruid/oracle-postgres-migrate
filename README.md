# Oracle to PostgreSQL

Migrator script for a existing oracle database to an existing but empty PostgreSQL database.

## When to use this?

For any reason you can't use csv import / export methods or any ETL tools.
You can use this script if you want more control over validation when inserting new records.

## How to use this?

### 1) Prepare two databases

Migrator assumes you have two identical databases in terms of datatype of columns, name of columns, constraints for each column and name of the table.
In some cases you can write your own validation rules if there is a conflict. Some conflicts are handled by migrator:

#### When you are inserting an integer in a boolean field

#### When you are inserting a null value in a char field which has non-null value constraint

You can write your own rules in `_check_custom_rules` method in `Validator` class.

### 2) Set environment variables

There are two sets of variable you should set in .env file. One for each database connection.

#### `PostgreSQL env variables`

`POSTGRES_URL = postgres://user:pass@localhost:5432/db_name`

#### `Oracle env variables`

`ORACLE_URL = localhost`
`ORACLE_PORT = 1521`
`ORACLE_SERVICE = service_name`
`ORACLE_USER = user`
`ORACLE_PASS = pass`
