# Pragmatic Data Platform Ingestion layer 
Welcome to the ingestion layer of the
Pragmatic Data Platform (PDP) package.

## Ingestion layer documentation
Ingestion of files into Landing Tables in the Pragmatic Data Platform is based on two operations:
1. creating the landing table, if not exists
2. ingestion of all new files since the last ingestion

For details on the parameters, the format of the dictionaries and basic examples of use
of the macros in this section, please look at their definition below in this file.

For extended examples of use you can look in the [ingestion](models/ingestion) folder 
in the Integration Tests part of this repository.

### Base Macros
To automate the ingestion operations there are the following three base macros:

#### create_landing_table_sql(...)
By passing a dictionary with the table name components (db, schema and name)
and the column definition (name and eventual type & not null contraint)
the macro produces the SQL code to create -if not exists already-
or recreate -if forced by the `recreate_table` parameter-
the Landing Table in the desired position and shape.

#### ingest_into_landing_sql(...)
Creates the COPY INTO statement to ingest the desired CSV files into the designated Landing Table.
The CSV specific feature is that we just need the number of columns, but not their names.

#### ingest_semi_structured_into_landing_sql(...)
Creates the COPY INTO statement to ingest the desired SEMI-STRUCTURED files into the designated Landing Table.
The SEMI-STRUCTURED specific feature is that in the `field_definitions`parameter
we need the name of the target columns and the expression to extrat each from the $1 variant column.

### Process Macros
The original Pragmatic Data Platform playbook was to use the above macros to create very clean macros
that ingested the desired data into a Landing Table. One macro for each LT.

Given the repetitions in these macros and to simplyfy even more the usage of the core PDP ingestion macros
we have crystallized the most common way to use the base macro in the following two macros (one for CSV,
one for SEMI-STRUCTURED formats) that take the input needed for the base macros and automate the ingestion process.
The input is passed in two dictionaries that describe the Landing Table and the files to load.

#### run_CSV_ingestion(...)
Creates one landing table -if not exists- and ingests the data from the designated files.
This macro is a wrapper that executes the Base Macros and logs their output.

#### run_SEMI_STRUCT_ingestion(...)
Creates one landing table -if not exists- and ingests the data from the designated files.
This macro is a wrapper that executes the Base Macros and logs their output.
