# -*- coding: utf-8 -*-

import yaml
import psycopg2
import os
import os.path
import sys
import logging
from pandas import read_csv, isnull
from mined.utils import logger, list_files, create_pgconn, BASE_DIR, SQL_DIR


def cleaning(folder_number):
    """
    Changes the column types of the joined table of folder_number, as well as perform any necessary cleaning on the columns
    The column types are changed following the {folder_number}_column_mapping_types.csv or the {folder_number}_column_mapping.csv columns.
    For boolean columns, it transforms Sí/Si/1 to True and No/0 to False.
    The additional cleaning is done from user-created SQL: SQL_DIR + f'/cleaning_{folder_number}{string_pattern}.sql'
    The format of this SQL is important, it needs to be a series of statements to be included inside a SELECT clause, optionally followed by two newlines and a statement to be included inside a WHERE clause
    If the preproc table has a 'year' column, it gets transformed into a year_range column as a date_range.
    
    Args:
        folder_number (int): the folder number, used for adding column type changes specific to that folder
    """
    folder_number = int(folder_number)
    connection, cursor = create_pgconn()
    cursor.execute('set role el_salvador_mined_education_write')
        
    with open(BASE_DIR + '/mined/ETL/config.yaml') as config_file:
        configs = yaml.load(config_file)
    
    if folder_number in configs['join_table_string']:
        string_patterns = configs['join_table_string'][folder_number]
    else:
        string_patterns = ['']
    
    for string_pattern in string_patterns:
        clean_name = f'{folder_number}_cleaned' + string_pattern
        preproc_name = f'{folder_number}_joined' + string_pattern
        
        if folder_number in configs['column_mode_1']:
            file_name = f"""{configs['base_dir']}/{configs['pre_dir']}/{folder_number}_column_mapping_types.csv"""
            indices = (0, 1)
        else:
            file_name = f"""{configs['base_dir']}/{configs['pre_dir']}/{folder_number}_column_mapping.csv"""
            indices = (1, 2)

        if not os.path.isfile(file_name):
            # TODO: make this print out the columns in the preproc table
            logger.error(f'File does not exist: {file_name}')
            cursor.execute(f"select column_name from information_schema.columns where table_name='{preproc_name}' and table_schema='preproc';")
            columns = [x[0] for x in cursor.fetchall()]
            col_types = dict()
        else:
            col_file = read_csv(file_name, header=None)
            columns = []
            col_types = dict()
            for idx, row in col_file.iterrows():
                if not isnull(row[indices[0]]):
                    columns += [row[indices[0]]]
                    if not isnull(row[indices[1]]):
                        col_types[row[indices[0]]] = row[indices[1]]
            columns = set(columns)
            columns = list(columns)
            columns.sort()


        # cleaning_{folder_number}.sql contains cleaning conditions and where conditions separated by ('\n\n')
        sql_cmnd = ''
        cleaned_columns = []
        split = []
        if os.path.isfile(SQL_DIR + f'/cleaning_{folder_number}{string_pattern}.sql'):
            with open(SQL_DIR + f'/cleaning_{folder_number}{string_pattern}.sql') as fil:
                sql_file = fil.read()
            split = sql_file.split('\n\n')
            sql_cmnd += split[0] # add the cleaning conditions to sql_command
            for line in split[0].split(' as ')[1:]: # retrieves the new column names created by cleaning conditions ['col_name,\n']
                split2 = line.split(',') 
                cleaned_columns += [split2[0]] # retrieve only col_name

        for col in columns:
            if col not in cleaned_columns:
                if col in col_types:
                    if col_types[col] == 'bool':
                        # TODO: generalise this piece of code, so that it reads from the preproc table the two options for the boolean variable
                        sql_cmnd += f"""CASE NULLIF({col},'') 
                                WHEN 'Sí' THEN '1'::bool
                                WHEN 'Si' THEN '1'::bool
                                WHEN 'No' THEN '0'::bool
                                WHEN '1' then '1'::bool
                                WHEN '0' then '0'::bool
                                END as {col},"""
                    elif 'int' in col_types[col]:
                        sql_cmnd += f"""NULLIF({col},'')::numeric::{col_types[col]} as {col},"""
                    else:
                        sql_cmnd += f"""NULLIF({col},'')::{col_types[col]} as {col},"""
                elif col == 'year':
                    sql_cmnd += f"""daterange(to_date(NULLIF(year,''), 'YYYY'), (to_date(NULLIF(year,''), 'YYYY') + interval '1 year')::date, '[)') as year_range,"""
                else:
                    sql_cmnd += f"""NULLIF({col},'') as {col},"""

        sql_cmnd = sql_cmnd.rstrip(',')
        logger.debug('Dropping table')
        cursor.execute(f"""DROP TABLE IF EXISTS cleaned."{clean_name}";""")
        connection.commit()
        logger.debug('Committed connection')

        logger.debug('Creating table')
        
        if len(split)>1:
            logger.debug(f'\tUsing:\nCREATE TABLE cleaned."{clean_name}" AS SELECT {sql_cmnd} FROM preproc."{preproc_name}" where {split[1]};')
            cursor.execute(f"""CREATE TABLE cleaned."{clean_name}" AS SELECT {sql_cmnd} FROM preproc."{preproc_name}" where {split[1]};""")
        else:
            logger.debug(f'\tUsing:\nCREATE TABLE cleaned."{clean_name}" AS SELECT {sql_cmnd} FROM preproc."{preproc_name}";')
            cursor.execute(f"""CREATE TABLE cleaned."{clean_name}" AS SELECT {sql_cmnd} FROM preproc."{preproc_name}";""")
        connection.commit()
        logger.debug('Committed connection')

    
if __name__ == "__main__":
    cleaning(sys.argv[1])
