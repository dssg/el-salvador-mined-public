## Inserting files into schema raw
**Assumption:** files have been converted into carrot separate values *and* are split into _header.csv, _body.csv, and .csv files
+ Files within the same folder have inconsistent headers to address this tables are a 1:1 ratio, file:table
+ Script to run: python insert_into_postgres.py

### insert_into_postgres.py
+ Config file values that need to be updated for each folder:
	+ **col_letter:** each folder is assigned its own letter. Below are the folder # to column letter mapping:
		+ 1: g
		+ 2: a
		+ 3: a
		+ 4: a
		+ 5: a
		+ 6: f
		+ 7: h
		+ 8: i
	+ **data_dir:** top level folder of where files are
	+ **folder_number:** folder number added as a prefix to table names. Syntax - '#_'
	+ **mode:** some folders have specific parsing rules that are different from others. Mode values:
		+ 1 - for folder 1
		+ 6 - for folder 6
		+ 7 - for folder 7
		+ 8 - for folder 8
		+ 0 - for every other folder
+ When insert_into_postgres.py is run it completes the following:
1. Generates CREATE_TABLE.sql for each file using csvsql
2. Changes all datatypes in CREATE_TABLE.sql into VARCHAR
3. Generats NEW_CREATE_TABLE.sql which renames each column as a letter and generates a new_mapped_table.csv file
	+ new_mapped_table.csv has 3 columns: original_col_name (generate from CREATE_TABLE.sql in step 1), mapped_col_name (generated from NEW_CREATE_TABLE.sql in step 2), and table_name
4. Runs NEW_CREATE_TABLE.sql to generate table in raw schema
5. Inserts data into created table



## Creating cleaned.student_labelled table
**Objective:** combine all the raw."6_*" tables (which contain student registration data).
<br>**Note:** tables in raw schema are a 1:1 ratio, table:file; this was to address the inconsistent column formats between files of the same type
