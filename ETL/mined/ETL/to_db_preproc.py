# -*- coding: utf-8 -*-

import yaml
import psycopg2
import os
import os.path
import sys
import logging
from pandas import read_csv, isnull
import re

from mined.utils import logger, create_pgconn, BASE_DIR


def unique_original_column_names(folder_number):
    """
        Returns the unique column names in the original .xls, .xlsx, .csv or .sav files
        Given a folder_number, it looks inside the raw.column_mapping table and returns the distinct original column names
        
        Args:
            folder_number (int): the folder number, used for finding the relevant rows in raw.column_mapping
    """
    connection, cursor = create_pgconn()
    cursor.execute("set role el_salvador_mined_education_write")
    cursor.execute(f"""select distinct(original_col_name) from raw.column_mapping where table_name ~ 'raw."{folder_number}.*';""" )
    col_names = [x[0] for x in cur.fetchall()]
    col_names.sort()
    return col_names


def join_tables(folder_number, create_table=True):
    """
        Finds all tables of some particular folder inside raw schema and joins them into a single table in preproc schema.
        In the conversion, the columns are renamed using .csv that need to be manually created in garfield.
        The column renaming can occur in one of two ways, as specified by the config file:
            1. The .csv file provides three columns: old column names, new column names and types.
            2. Two .csv files are provided, one for column names and one for column types. Each row in the .csv's corresponds to a table in raw, and the first column should be the table name. The rest of the columns correspond to the columns in the raw.table, e.g. if you want the third column of raw.super_table to be renamed as something_useful, the .csv should have a row in which the first column is raw.super_table and the fourth column is something_useful. Similarly for types.
        In either mode, if the new column name is not included, it will not appear in the joined table. If the data type is not included, it will be treated as varchar or handled as a special case in change_column_types()
        
        Args:
            folder_number (int): the folder number, used for finding the .csv and the tables in the raw schema
            create_table (bool): whether to create the joined table and populate it
    """
    folder_number = int(folder_number)
    with open(BASE_DIR + '/mined/ETL/config.yaml') as config_file:
        configs = yaml.load(config_file)
    
    # Checking whether the column mapping .csv exists. If not, it prints the names of the original columns and returns None
    # With these original column names, the user can create a .csv that maps the original column names to the desired column names for the database
    if not os.path.isfile(f"""{configs['base_dir']}/{configs['pre_dir']}/{folder_number}_column_mapping.csv"""):
        logger.warn(f"""To join tables you need to create: {folder_number}_column_mapping.csv""")
        [print(x) for x in unique_original_column_names(folder_number)]
        return
    
    conn, cur = create_pgconn()
    cur.execute("set role el_salvador_mined_education_write")
    
    # Some folders contain information about semantically different entities (e.g. folder 1 contains school information but also some aggregate descriptive excels). However, each table has keywords identifying what information they contain (e.g. for folder 1, the keyword is Base_de_Centros). This keyword can be used to join each group of tables into separate tables in the preprocessing schema. This only works for tables that are joined in column_mode_1, where the file_name is included in the csv, allowing the script to select the appropriate columns for the tables being joined.
    if folder_number in configs['join_table_string']:
        string_patterns = configs['join_table_string'][folder_number]
    else:
        string_patterns = ['']
    
    for string_pattern in string_patterns:
        if folder_number in configs['column_mode_1']:
            joined_name = f'{folder_number}_joined' + string_pattern
            # The renaming uses the ordering of the new column names in 1_column_mapping.csv, combined with the automatic naming of columns by csvkit (a,b,c...aa,bb,cc...aaa,bbb,...)
            with open(f"""{configs['base_dir']}/{configs['pre_dir']}/{folder_number}_column_mapping.csv""", 'r') as fil:
                col_file = fil.read()
            col_map = dict()
            all_new_cols = []
            for pair in col_file.split('\n'):
                lst = pair.split(',')
                key = f'raw."{lst[0]}"'
                # The first column in the csv is meant to the file name, so only select those rows for which we have specified the keyword (string_pattern)
                if string_pattern in key.lower():
                    col_map[key] = dict()
                    new = []
                    mapped = []
                    for idx, new_col in enumerate(lst[1:]):
                        if new_col != '':
                            mapped += ['%s%d' %(chr(96+folder_number), idx)]
                            new += [new_col]
                    col_map[key]['mapped'] = mapped
                    col_map[key]['new'] = new
                    all_new_cols += new
            union = set(all_new_cols)
            create_table_stmnt = 'original_table varchar, year varchar,' + ' varchar, '.join(union) + ' varchar' 
        else:
            logger.debug(f"""Opening {folder_number}_column_mapping.csv""")
            with open(f"""{configs['base_dir']}/{configs['pre_dir']}/{folder_number}_column_mapping.csv""", 'r') as fil:
                col_map = fil.read()
            # Maps the old column names to the new column names and types, using the .csv file. It takes into account that not all old columns will have names (i.e. we leave them behind in the data), and that not all named columns have types (i.e. they stay as varchars)
            logger.debug(f'Creating mapping dictionary')
            col_names=dict()
            col_types=dict()
            union = []
            for pair in col_map.split('\n'):
                split_pair = pair.split(',')
                if split_pair[1] != '':
                    col_names[split_pair[0]] = split_pair[1]
                    if len(split_pair)>2:
                        if split_pair[2] != '':
                            col_types[split_pair[1]] = split_pair[2]
                    union += [split_pair[1]]
            union = set(union)  # In case we want multiple old columns names (in different files) to get remapped to the same new column
            union=set(union).difference(set(['']))
            create_table_stmnt = 'original_table varchar,' + ' varchar, '.join(union) + ' varchar'  

        if create_table:
            logger.debug(f'Fetching list of table names in raw')
            cur.execute(f"""select distinct(table_name) from raw.column_mapping where lower(table_name) ~ 'raw."{folder_number}.*{string_pattern}.*';""")
            table_list = [x[0] for x in cur.fetchall()]
            logger.debug(f'Creating joined table with renamed columns. Dropping it if it exists')
            cur.execute(f"""drop table if exists preproc."{joined_name}";""")
            logger.debug(f'Creating table')
            cur.execute(f"""create table if not exists preproc."{joined_name}" ({create_table_stmnt});""")

            # Iterates over all tables in raw (with columns a0, a1, a2...), inserting into a joined preprocessing table (with columns nie, dpto_code_ce, year...)
            logger.debug('Iterating over tables and inserting into joined table, with renamed columns')
            for table in table_list:
                logger.debug(f'    {table}')
                if folder_number in configs['column_mode_1']:
                    cols = col_map[table]
                    if len(cols['new'])>0:
                        year = re.findall('[0-9]{4}', table)[0] # Extracts the year from the file_name
                        logger.debug(f'Inserting into {joined_name}, with year {year}')
                        cur.execute(f"""insert into preproc."{joined_name}" (original_table, year, {','.join(cols['new'])}) select {"'" + table + "' as original_table," + year + ',' + ','.join(cols['mapped'])} from {table};""")                
                else:
                    cur.execute(f"""select mapped_col_name, original_col_name from raw.column_mapping where table_name='{table}' order by mapped_col_name;""")
                    col_pairs = cur.fetchall()
                    raw_cols = []
                    renamed_cols = []
                    for col_pair in col_pairs:
                        new_name=col_pair[1].replace('"', '')
                        if new_name in col_names:
                            if col_names[new_name] != '':
                                raw_cols += [col_pair[0]]
                                renamed_cols += [col_names[new_name]]
                    cmnd = ' varchar, '.join(renamed_cols) + ' varchar'
                    cur.execute(f"""insert into preproc."{joined_name}" ({'original_table,' + ','.join(renamed_cols)}) select {"'" + table[5:-1] + "' as original_table," + ','.join(raw_cols)} from {table};""")
            logger.debug(f'Finished inserting into {joined_name}')
            conn.commit()
            logger.debug('Committed connection')        

        
if __name__ == "__main__":
    join_tables(sys.argv[1])
