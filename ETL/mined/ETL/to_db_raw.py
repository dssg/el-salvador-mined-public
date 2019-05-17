# -*- coding: utf-8 -*-

import os
import os.path
import subprocess
import psycopg2
import yaml
import logging
import sys
from pandas import read_csv
from mined.utils import logger, list_files, create_pgconn, BASE_DIR

                  
def main(folder_number):
    """
    There are three steps for this script:
    1. Goes into the folder <pre_dir/folder_number> and iterates over all the .csv files in them (either , or ^ delimiter).
    2. For each file, it uploads the _body.csv to the `raw` schema, with column names a0,a1,a2... for folder 1; b0,b1,b2... for folder 2; etc
    3. It then, for each file, gets the _head.csv, and maps the new column names (a0, a1, b0, c0...) to the old column names (anio, status, departamento...)
    """
    
    ## Declaring which directory to process
    ## This is taken from yaml config file called 'ETL.yml'
    with open(BASE_DIR + '/mined/ETL/config.yaml') as config_file:
        yml_dict = yaml.load(config_file)
    
    ## variables for retrieving files
    data_parent_dir = os.path.join(yml_dict['base_dir'], yml_dict['pre_dir'])
    data_dir = yml_dict['folder_dictionary'][folder_number]['folder']
    working_dir = os.path.join(data_parent_dir, data_dir)
    ext = yml_dict['extension']
    delim = yml_dict['folder_dictionary']['comma_separated']
    schema = yml_dict['schema_name']
    if folder_number in delim:
        delim = ','
    else:
        delim = '^'
        
    ## Creating connection to postgres
    logger.info('Connecting to postgres')
    connection, cursor = create_pgconn()
    cursor.execute("set role el_salvador_mined_education_write")

    # START! Retrieve files from specified directory in datos
    logger.info('Retrieving body csv files')
    csv_files = list_files(working_dir, [ext])

    if not csv_files:
        logger.debug('ERROR: no files retrieved. Exiting program')
    
    logger.debug('Files retrieved: {}'.format(csv_files))
    
    for csv_f in csv_files:
        file_path_bytes = csv_f.encode('latin-1', 'ignore')
        file_path_str = file_path_bytes.decode('utf-8')
        
        relative_path = file_path_str.replace(data_parent_dir, '') # Retrieves up to 'data_parent_dir'
        split_path = relative_path.split('/')
        table_name = str(folder_number) + '_' + '_'.join(split_path[1:]).replace(ext, '')
        table_name_with_schema = schema + '"' + table_name + '"'
        
        logger.info('Removing column mapping data')
        cursor.execute(f"""select distinct(table_name) from raw.column_mapping where table_name ~ '{schema}"{folder_number}_*'""")
        fetch = cursor.fetchall() # fetchall() returns a list of tuples w/ 2 items in each tuple
        column_mapping_column_list = [string for tup in fetch for string in tup]
        if table_name_with_schema in column_mapping_column_list:
            cursor.execute(f"""delete from raw.column_mapping where table_name = '{table_name_with_schema}'""")
        else:
            pass
        
        # TODO: check if base_filename is > 63 characters due to Postgres tablename limit
        
        logger.info('Starting on file: {}'.format(csv_f.split('/')[-1]))
        logger.info('Table name will be: {}'.format(table_name_with_schema))

        ## Generate CREATE TABLE STATEMENT
        logger.info('Generating create table statement for table {}'.format(table_name_with_schema))
        csv_path_head = csv_f.replace(ext, '_head.csv')
        headers_list = list(read_csv(csv_path_head, sep=delim).columns)
        headers_list = [h.lower().replace("'", "") for h in headers_list]
        col_letter = chr(96 + folder_number) 
        new_col_header = [col_letter + str(head) for head in range(len(headers_list))]
        drop_table_query = f"""DROP TABLE IF EXISTS {table_name_with_schema};""" 
        create_table_query = f"""CREATE TABLE {table_name_with_schema} ({' VARCHAR, '.join(new_col_header) + ' VARCHAR'});"""
        logger.debug(create_table_query)
        column_mapping_data = zip(headers_list, new_col_header, [table_name_with_schema]*len(headers_list))

        ## Creating table.
        logger.info('Dropping table {}'.format(table_name_with_schema))
        cursor.execute(drop_table_query)
        logger.debug('Drop table execution message: ()'.format(cursor.statusmessage))
        logger.info('Executing create table statement {}'.format(table_name_with_schema))
        cursor.execute(create_table_query)
        logging.debug('Create table execution message: {}'.format(cursor.statusmessage))
        
        ## Inserting data
        logger.info('Inserting into table')
        with open(csv_f, 'r') as csv_file:
            if folder_number in yml_dict['folder_dictionary']['comma_separated']:
                cursor.copy_expert(f"""COPY {table_name_with_schema} FROM STDIN WITH CSV DELIMITER ',' QUOTE '"' NULL '' """, csv_file)
            else:
                cursor.copy_expert(f"""COPY {table_name_with_schema} FROM STDIN WITH CSV DELIMITER '^' NULL '' """, csv_file)

            cursor.execute(f"""select count(*) from {table_name_with_schema};""")
            logging.info('Count on table {}: {}'.format(table_name_with_schema, cursor.fetchone()))
        
        ## Inserting column mapping data
        logger.info('Inserting column mapping data into col_mapping table')
        #TODO: should the tablename and col names in table be hard coded?
        for data in column_mapping_data:
            insert_query = f"""INSERT INTO raw.column_mapping(original_col_name, mapped_col_name, table_name) VALUES ('{data[0]}', '{data[1]}', '{data[2]}' );"""
            cursor.execute(insert_query)
        connection.commit()
    connection.close()

    logger.info(f'{data_dir} folder is completed')
        
if __name__ == '__main__':
    main(int(sys.argv[1]))
