# -*- coding: utf-8 -*-

import os
import subprocess
import sys
import logging
from mined.utils import logger, list_files, SCRIPTS_DIR, BASE_DIR


def extract(directory, extensions, raw_folder, preprocessing_folder):
    """
        Finds all files with some extensions inside a directory and converts them into a carrot separated value files. 
        In the conversion, the files are moved from their initial raw_folder to a preprocessing folder, creating the necessary folder structure. Conversion requires 'xls_csv.sh', 'sav_csv.sh' and 'csv_csv.sh'
        Each .csv file is cleaned by removing newlines, trailing and leading spaces, and replacing consecutive spaces with a single space.
        Finally, each file is checked to see if all rows have the same number of fields using 'csv_health.sh'
        
        Args:
            directory (str): the folder containing the files you want to convert to csv
            extensions (lst): iterable of extension strings
            raw_folder (str): name of the folder containing raw data
            preprocessing_folder (str): name of the preprocessing folder
    """
    for extension in extensions:
        files = list_files(directory, [extension])
        for input_file in files:
            output_file = input_file.replace(raw_folder, preprocessing_folder)
            output_file = output_file.replace(extension, 'csv')
            
            try:
                if not os.path.exists(output_file):
                    logger.debug('Output file: %s' %output_file)
                    dirname = os.path.dirname(output_file)
                    if not os.path.exists(dirname):
                        os.makedirs(dirname)
                        logger.debug('Making directory: %s' %dirname)
                    if extension in ['xls', 'xlsx']:
                        subprocess.check_output(['bash', SCRIPTS_DIR + '/xls_csv.sh', input_file, output_file])
                        logger.debug('Converted xls to csv')
                    elif extension in ['sav']:
                        subprocess.check_output(['bash', SCRIPTS_DIR + '/sav_csv.sh', input_file, output_file])
                        logger.debug('Converted sav to csv')
                    elif extension in ['csv']:
                        subprocess.check_output(['bash', SCRIPTS_DIR + '/csv_csv.sh', input_file, output_file])
                        logger.debug('Converted csv to csv')
                    else:
                        logger.warn('Cannot do anything with %s extension' %extension)

                    healthy = subprocess.check_output(['bash', SCRIPTS_DIR + '/csv_health.sh', output_file])
                    if not healthy:
                        logger.warn('File is unhealthy: %s' %output_file)
            except Exception as e:
                logger.warn('File %s failed because %s' %(output_file, e))
                if os.path.exists(output_file):
                    subprocess.run(['rm', output_file])

def unzip(directory, extension, raw_folder, preprocessing_folder):
    """
        Unzips zip and 7z compressed files and moves them from a raw_folder to a preprocessing_folder
        
        Args:
            directory (str): the folder containing the files you want to unzip
            extensions (lst): iterable of extension strings
            raw_folder (str): name of the folder containing raw data
            preprocessing_folder (str): name of the preprocessing folder
    """
                                
    files = list_files(directory, [extension])
    for file_name in files:
        target_directory = os.path.dirname(file_name.replace(raw_folder, preprocessing_folder))
        if not os.path.exists(target_directory):
            os.makedirs(target_directory)
            logger.debug('Making directory: %s' %target_directory)
        if extension in ['zip']:
            subprocess.check_output(['unzip', file_name, '-d', target_directory])
        elif extension in ['7z']:
            subprocess.check_output(['7z', 'e', file_name, '-o%s' %target_directory])
        
def check_csv_health(directory):
    """
        Iterates over a directory, finding csv files and checking that they have the same number of fields per row, using 'csv_health.sh'
        
        Args:
            directory (str): the folder containing the files you want to check
    """
                                
    files = list_files(directory, ['csv'])
    for input_file in files:
        print(input_file, subprocess.check_output(['bash', SCRIPTS_DIR + '/csv_health.sh', input_file]))

def split_csv(directory, auto_split=1, threshold_row=25):
    """
        Iterates over csv files in a directory, splitting them into headers and body (if those files do not already exist).
        Uses 'header_split.sh'. If auto_split=1, 'header_split.sh' attempts to automatically detect the header row by finding the first row where all the elements are full except for 'threshold_row'. 
        If auto_split=0, 'threshold_row' is the row at which the splitting occurs
        
        Args:
            directory (str): the folder containing the files you want to split
            auto_split (int): 1 for automatic splitting, 0 for fixed split.
            threshold_row (int): either the number of empty cells in the header row (if auto_split=1) or the row at which to split the csv (if auto_split=0)
    """

    files = list_files(directory, ['csv'])
    for file_name in files:
        if not file_name.endswith('_body.csv') and not file_name.endswith('_head.csv'):
            if not os.path.exists(file_name[:-4] + '_body.csv') and not os.path.exists(file_name[:-4] + '_head.csv'):
                subprocess.check_output(['bash', SCRIPTS_DIR + '/header_split.sh', file_name[:-4], str(auto_split), str(threshold_row)])
                logger.debug('Split csv %s' %file_name)


if __name__ == "__main__":
    import yaml
    with open(BASE_DIR + '/mined/ETL/config.yaml') as config_file:
        config = yaml.load(config_file)
    folder_num = int(sys.argv[1])
    folder_dict = config['folder_dictionary'][folder_num]
    raw_path = config['base_dir'] + config['raw_dir']
    
    full_path = raw_path + folder_dict['folder']
    logger.debug(f"Extracting {folder_dict['file_types']} from {config['pre_dir']}")
    extract(full_path, folder_dict['file_types'], config['raw_dir'], config['pre_dir'])
    
    logger.debug(f"Splitting header and body")
    if folder_dict['header_split'] == 'auto':
        split_csv(full_path.replace(config['raw_dir'], config['pre_dir']))
    else:
        split_csv(full_path.replace(config['raw_dir'], config['pre_dir']), 0, folder_dict['header_split'])

    '''FOLDER 1'''
    if '1' in sys.argv:
        for file in list_files(full_path.replace(config['raw_dir'], config['pre_dir']), ['csv']):
            if '_-_' in file:
                os.remove(file)

    '''FOLDER 8'''
    if '8' in sys.argv:
        for file in list_files(full_path.replace(config['raw_dir'], config['pre_dir']), 'csv'):
            if '~$' in file in file:
                os.remove(file)
