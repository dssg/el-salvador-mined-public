# -*- coding: utf-8 -*-

import yaml
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DateRange
import os, sys
import os.path
import logging
import re
import pandas as pd
from io import StringIO
import datetime

from dotenv import load_dotenv
from pathlib import Path
env_path = Path('.') / '.env'
load_dotenv(env_path)

# Set the path
home_dir = os.getcwd()

PGHOST=os.getenv("PGHOST")
PGPORT=os.getenv("PGPORT")
PGDATABASE=os.getenv("PGDATABASE")
PGUSER=os.getenv("PGUSER")
PGPASSWORD=os.getenv("PGPASSWORD")
DB_ROLE=os.getenv("POSTGRES_ROLE")

CONFIG_DIR=os.getenv("CONFIG_DIR")
SQL_DIR=os.getenv("SQL_DIR")
EXPERIMENTS_DIR=os.getenv("EXPERIMENTS_DIR")
MATRICES_DIR=os.getenv("MATRICES_DIR")
MODELS_DIR=os.getenv("MODELS_DIR")
PLOTS_DIR=os.getenv("PLOTS_DIR")
SCRIPTS_DIR=os.getenv("SCRIPTS_DIR")
BASE_DIR = os.getenv("MINED_ROOT_DIR")

def list_files(startpath, file_ext=[]):
    files_list = []
    for path, dirs, files in os.walk(startpath):
        for ext in file_ext:
            for f in files:
                if f.endswith(ext):
                    files_list.append(os.path.join(path, f))
                else:
                    continue
    return files_list


def create_pgconn():
    try:
        conn = psycopg2.connect(f"dbname='{PGDATABASE}' user='{PGUSER}' host='{PGHOST}' password='{PGPASSWORD}'")
    except Exception as e:
        print("Error connecting to db.")
        raise e
    cur = conn.cursor()
    cur.execute(f"SET ROLE {DB_ROLE}")
    return conn, cur


def parse_date_string(date_string):
    """
    Function that given time interval string eg: '1d'
    Returns a dictionary with the string time intervals
    as key and the relative deltas as values
    Args:
      date_string (str): time interval string
    """
    dict_abbreviations = {'h':'hours',
                          'd':'days',
                          'w':'weeks',
                          'm':'months',
                          'M':'months',
                          'y':'years',
                          'Y':'years'}
    time_delta = {}
    try:
        try:
            unit = re.findall(r'\d+(\w)', date_string)[0]
        except:
            raise ValueError('Could not parse units from prediction_window string')

        try:
            value = int(re.findall(r'\d+', date_string)[0])
        except:
            raise ValueError('Could not parse value from prediction_window string')

        time_delta[dict_abbreviations[unit]] = value
    except:
        raise ValueError('Could not parse value for window')

    return time_delta


'''COLORED LOGGING'''
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

# These are the sequences need to get colored ouput
RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"

def formatter_message(message, use_color = True):
    if use_color:
        message = message.replace("$RESET", RESET_SEQ).replace("$BOLD", BOLD_SEQ)
    else:
        message = message.replace("$RESET", "").replace("$BOLD", "")
    return message

COLORS = {
    'WARNING': YELLOW,
    'INFO': WHITE,
    'DEBUG': BLUE,
    'CRITICAL': YELLOW,
    'ERROR': RED
}

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        levelname = record.levelname
        if levelname in COLORS:
            levelname_color = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
            record.levelname = levelname_color
        return logging.Formatter.format(self, record)

def create_logger(name='Experiment', **kwargs):
    '''This functions defines the Logger called Experiment, with the relevant colored formatting'''
    if 'level' in kwargs:
        LOGGER_LEVEL = kwargs['level']
    else:
        LOGGER_LEVEL = 'DEBUG'
    if 'filename' in kwargs:
        LOGGER_FILE = kwargs['filename']
    else:
        LOGGER_FILE = None
    fh = logging.StreamHandler(sys.stdout)
    f = ColoredFormatter('[%(name)s] - %(levelname)s - %(asctime)s: %(message)s ', '%H:%M')
    fh.setFormatter(f)
    test = logging.getLogger(name)
    test.propagate = False
    test.setLevel(LOGGER_LEVEL)
    if not len(test.handlers):
        test.addHandler(fh)

    if LOGGER_FILE is not None:
        fh = logging.FileHandler(LOGGER_FILE)
        fh.setFormatter(logging.Formatter('[%(name)s] - %(levelname)s: %(message)s - %(asctime)s ', datefmt='%H:%M'))
        fh.setLevel(LOGGER_LEVEL)
        test.addHandler(fh)

    return test

logger=create_logger()

def store_predictions(scores_with_index, experiment, model_index):
    conn, cursor = create_pgconn()
    cursor.execute('set role el_salvador_mined_education_write;')
    q = ''
    for score in scores_with_index:
        #q += f"""'{experiment}'\t'{model_index}'\t'{scores_with_index[index][0][1]}'\t'{DateRange(lower=scores_with_index[index][0][0].lower.strftime('%Y-%m-%d'), upper=scores_with_index[index][0][0].upper.strftime('%Y-%m-%d'), bounds='[)')}'\t{str(scores_with_index[0][1])}\t'{datetime.datetime.today().strftime('%Y/%m/%d %H:%M.%S')}'\n"""
       q += str(experiment) + '\t' + str(model_index) + '\t' + str(score[0][1]) + '\t' + str(DateRange(lower=score[0][0].lower.strftime('%Y-%m-%d'), upper=score[0][0].upper.strftime('%Y-%m-%d'), bounds='[)')) + '\t' + str(score[1]) + '\t' + datetime.datetime.today().strftime('%Y/%m/%d %H:%M.%S')+ '\n'
    writer = StringIO(q)
    cursor.copy_from(writer, 'results.predictions', columns=('experiment', 'model', 'student', 'year', 'score', 'prediction_date'))
    writer.close()
    conn.commit()
    conn.close()
    logger.debug('Predictions stored. Connection close')
    return


def store_evaluations(metrics, experiment, model_index):
    conn, cursor = create_pgconn()
    cursor.execute('set role el_salvador_mined_education_write;')
    
    cursor.execute(f"""INSERT INTO results.metrics (
    experiment, model, precision_k, recall_k, auc, total_population, total_real_positive, total_real_negative
    )
    VALUES (
        '{experiment}', {model_index}, %s, %s, {metrics['auc']}, {metrics['total_pop']}, {metrics['real_pos']}, {metrics['real_neg']})""", (metrics['prec_k'], metrics['rec_k']))          
    conn.commit()
    conn.close()
    logger.debug('Evaulations stored. Connection closed.')
    return


def store_feature_importances(experiment, model_index, cols, importance, indices):
    feature_col_index_mapping = [(cols[i], i, importance[i]) for i in indices]
    conn, cursor = create_pgconn()
    cursor.execute('set role el_salvador_mined_education_write;')
    q = ''
    for index, item in enumerate(feature_col_index_mapping):
        q += str(experiment) + '\t' + str(model_index) + '\t' + str(item[0]) + '\t' + str(item[2]) + '\n'
    writer = StringIO(q)
    cursor.copy_from(writer, 'results.feature_importances', columns=('experiment', 'model', 'feature_name', 'score'))
    writer.close()
    conn.commit()
    conn.close()
    logger.debug('Evaulations stored. Connection closed.')                       
