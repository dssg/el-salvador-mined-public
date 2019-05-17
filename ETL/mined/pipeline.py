# -*- coding: utf-8 -*-

## PIPELINE OF ML

import yaml
import psycopg2
import os
import os.path
import sys
import pandas as pd
import numpy as np
from pandas import read_csv, isnull
import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
from pprint import pprint
import random
import json
import git
import collections
from itertools import product
import gc


from mined.utils import create_pgconn, parse_date_string, logger, SQL_DIR, MATRICES_DIR
from mined.ml import precision_n, recall_n, auc, normalize_data


## Functions
def create_experiment(config_file):
    with open(config_file,  'r') as f:
        configs = yaml.load(f)
        experiment = hash(str(configs)) % ((sys.maxsize + 1) * 2)
              
        conn, cur = create_pgconn()
        cur.execute('set role el_salvador_mined_education_write')
        
        df = pd.read_sql(f"""SELECT experiment 
                         FROM results.experiments 
                         WHERE experiment = '{experiment}'""", conn)
        if df.empty:
            repo = git.Repo(search_parent_directories=True)
            git_hash = repo.head.object.hexsha
            config_json = json.dumps(configs, indent=4, sort_keys=True, default=str)
            #features = json.dumps(configs['features'], indent=4, sort_keys=True, default=str)
            seed = random.randint(0, 1e6)
                        
            cur.execute(f"""INSERT INTO results.experiments
                (
                experiment,
                git_hash,
                config,
                run_date,
                seed
                )
            VALUES
                (
                '{experiment}',
                '{git_hash}',
                '{config_json}',
                 now(),
                '{seed}'
                )"""
            )
            conn.commit()

        else:
            logger.debug('Experiment already exists.')
            
        return experiment, configs

def calculate_splits(config_dict, experiment, rerun=False):
    """
        Creates a table in staging with each row being a split.

        :param config_dict: dictionary containing data_start, data_end, train_window, test_window, label_window, and step
        :type dict:
        :param experiment: experiment identifier
        :type str:

        :return: data frame containing the experiment and the following time ranges: train_span, test_span, train_label_span, test_label_span
        :rtype: data_frame
    """

    if rerun:
        conn, cur = create_pgconn()
        cur.execute('set role el_salvador_mined_education_write;')
        return pd.read_sql(f"""SELECT * FROM results.splits where experiment='{experiment}';""", conn)

    logger.debug(config_dict['temporal_splits'])

    split_dict = config_dict['temporal_splits']
    data_start = split_dict['data_starts']
    data_end = split_dict['data_ends']
    index = 0
    for train_window, test_window, label_window, step in product(split_dict['train_window'], 
                                                                 split_dict['test_window'], 
                                                                 split_dict['label_window'], 
                                                                 split_dict['step']):
        train_window = relativedelta(**parse_date_string(train_window))
        test_window = relativedelta(**parse_date_string(test_window))
        label_window = relativedelta(**parse_date_string(label_window))
        step = relativedelta(**parse_date_string(step))
        
        test_end = data_end - label_window
        #test_start = test_end - test_window
        test_start = test_end - label_window
        train_end = test_end - label_window
        
        conn, cur = create_pgconn()
        while train_end > data_start:
            train_start = train_end - train_window
            if train_start < data_start:
                train_start = data_start
            logger.debug('SPLIT')
            logger.debug(f'\tTrain span: {train_start} {train_end}\n\tTest span: {test_start} {test_end}\n\tTrain label span: {train_start} {train_end + label_window}\n\tTest label span: {test_start} {test_end + label_window}')
            cur.execute("set role el_salvador_mined_education_write;")
            cur.execute(f"""INSERT INTO results.splits
                (
                split,
                experiment,
                train_span,
                test_span,
                train_label_span,
                test_label_span
                )
            VALUES
                (
                {index},
                '{experiment}',
                daterange('{train_start}', '{train_end}', '[)'),
                daterange('{test_start}', '{test_end}', '[]'),
                daterange('{train_start}', '{train_end + label_window}', '[)'),
                daterange('{test_start}', '{test_end + label_window}', '[)')
                )"""
                       )
            index += 1
            test_end -= step
            #test_start = test_end - test_window
            test_start = test_end - label_window
            train_end = test_end - label_window
        conn.commit()
    return pd.read_sql(f"""SELECT * FROM results.splits where experiment='{experiment}'""", conn)


def get_matrices(split, config, experiment, split_index, debug=False):
    """
        Splits data into train and split, according to a time_range

        :param df: data
        :type df: data_frame
        :param time_range: start and end dates
        :type time_range: range

        :return: train
        :rtype: data_frame
        :return: test
        :rtype: data_frame
    """

#     features_table = config['features_table']
    grado = config['grado']
#     feature_zip = list(zip(['features.']*len(config['features']), config['features']))
#     feature_list = ','.join([''.join(feat) for feat in feature_zip])
    if debug:
        debug_string = '_debug'
    else:
        debug_string = ''
    
    index_train = split['train_span']
    index_test = split['test_span']
    low_train, high_train = index_train.lower, index_train.upper
    low_test, high_test = index_test.lower, index_test.upper
    
    feature_groups = config['features']
    all_features = set()
    for features in feature_groups.values():
        all_features = all_features.union(set(features))
    
    # For each feature group, create a CTE selecting the appropriate features over the appropriate amount of time for training and testing (train_sql and test_sql). After that, all the CTEs are left-joined on year and student, with all_features being selected (select_sql and final_sql). This fails if the different tables have features that are called the same.
    train_tmp = ''
    test_tmp = ''
    train_sql = None
    test_sql = None
    for feature_group, features in feature_groups.items():
        logger.debug(f"Extracting features from {feature_group}")
        train_tmp += f"create temp table {feature_group}_train as (select year_range, student, {','.join(features)} from results.{feature_group}{debug_string} WHERE year_range <@ daterange(quote_literal('{low_train}')::date, quote_literal('{high_train}')::date)); create index {feature_group}_train_idx on {feature_group}_train(year_range, student);"
        test_tmp += f"create temp table {feature_group}_test as (select year_range, student, {','.join(features)} from results.{feature_group}{debug_string} WHERE year_range <@ daterange(quote_literal('{low_test}')::date, quote_literal('{high_test}')::date)); create index {feature_group}_test_idx on {feature_group}_test(year_range, student);"
        if train_sql is None:
            train_sql = f" select year_range, student, {','.join(all_features)}, labels.label as label from {feature_group}_train a" # 100*random() as random_feature,
            test_sql = f" select year_range, student, {','.join(all_features)}, labels.label as label from {feature_group}_test a" # 100*random() as random_feature,
        else:
            train_sql += f" left join {feature_group}_train using (year_range, student)"
            test_sql += f" left join {feature_group}_test using (year_range, student)"
    
    if grado == '*':
        logger.debug('Running all grades')
        train_sql += f" left join staging.labels{debug_string} as labels using (year_range, student) where labels.label is not null;"
        test_sql += f" left join staging.labels{debug_string} as labels using (year_range, student) where labels.label is not null;"
    elif grado != '*':
        train_sql += f" left join staging.labels{debug_string} as labels using (year_range, student) where labels.label is not null and grado_code = {grado};"
        test_sql += f" left join staging.labels{debug_string} as labels using (year_range, student) where labels.label is not null and grado_code = {grado};"
    else:
        raise ValueError('Grado not recognised')
    train_sql = train_tmp + train_sql
    test_sql = test_tmp + test_sql
    conn, cursor = create_pgconn()
    cursor.execute('set role el_salvador_mined_education_write')

    t0 = datetime.datetime.now()
    logger.debug('Retrieving training generator')
    gener = pd.read_sql(train_sql, conn, chunksize=100000);
    logger.debug('Retrieving training dataframe')
    train = pd.concat(gener)
    logger.debug(f'TRAIN TIME CHUNKED: {datetime.datetime.now()-t0}')
    logger.debug(train.info(memory_usage='deep'))
    del gener
    gc.collect()

    t0 = datetime.datetime.now()    
    logger.debug('Retrieving testing generator')
    gener = pd.read_sql(test_sql, conn, chunksize=100000);
    logger.debug('Retrieving testing dataframe')
    test = pd.concat(gener)
    logger.debug(f'TEST TIME CHUNKED: {datetime.datetime.now()-t0}')
    logger.debug(test.info(memory_usage='deep'))
    del gener
    gc.collect()

    train.set_index(['year_range', 'student'], inplace=True)
    test.set_index(['year_range', 'student'], inplace=True)
    
    logger.debug('Training and testing dataframes stored. Connection closed.')
    
    train_cols = train.columns != train.columns[-1]
    test_cols = train.columns[-1]
    
    logger.debug("Creating train and test matrix split")
    X_train = train.loc[:, train_cols]
    X_train = X_train.fillna(value=-1)
    y_train = train.loc[:, test_cols]
    y_train = y_train.fillna(value=-1)
    logger.debug(f"Train size: X_train={X_train.shape}, y_train={y_train.shape}")

    X_test = test.loc[:, train_cols]
    X_test = X_test.fillna(value=-1)
    y_test = test.loc[:, test_cols]
    y_test = y_test.fillna(value=-1)
    
    del train, test
    gc.collect()
    
    logger.debug(f"Test size: X_test={X_test.shape}, y_test={y_test.shape}")
    
    pkl_list = [os.path.join(MATRICES_DIR, str(experiment) + "_" + str(split_index) + '_trainX.pkl'), os.path.join(MATRICES_DIR, str(experiment) + "_" + str(split_index) + '_trainY.pkl'), os.path.join(MATRICES_DIR, str(experiment) + "_" + str(split_index) + '_testX.pkl'), os.path.join(MATRICES_DIR, str(experiment) + "_" + str(split_index) + '_testY.pkl')]
    X_train.to_pickle(pkl_list[0])
    y_train.to_pickle(pkl_list[1])
    X_test.to_pickle(pkl_list[2])
    y_test.to_pickle(pkl_list[3])
    
    for pkl in pkl_list:
        pkl = f"'{pkl}'"
        file_name = f"'{os.path.basename(pkl)}"
        cursor.execute(f"""INSERT INTO results.matrices (experiment, matrix, split)
        VALUES (
            '{experiment}',
            {file_name},
            '{split_index}')""")
         
    conn.commit()
    conn.close()
    
    
    return X_train, y_train, X_test, y_test


def create_features(config_dict):
    """
        Creates features for ML. Receives a config dictionay which determines the location of the sql file

        :param sql_file: path to sql file with features
        :type df: string

        :return: None
    """

    sql_file = config_dict['features'][0] # NOTE: just for version 0.1
    logger.debug(SQL_DIR)
    with open(os.path.join(SQL_DIR, sql_file), 'r') as f:
        feature_sql = f.read()
    conn, cursor = create_pgconn()
    logger.debug('Generating features...')
    cursor.execute(feature_sql)
    cursor.execute("select count(*) from results.features;")
    logger.debug(f"Count on table {cursor.fetchone()}.")
    conn.commit()
    conn.close()
    logger.debug('Done generating features. Connection committed')


def create_labels(config_dict):
    """
        Creates labels for ML. Receives a config dictionary from which a label_window can be read and passed to the label SQL string.

        :param df: config_dict
        :type df: dict
    """

    with open(os.path.join(SQL_DIR, 'labeling.sql'), 'r') as fil:
        sql_cmnd = fil.read()
    label_window = parse_date_string(config_dict['temporal_splits']['label_window'])['years']
    sql_cmnd=sql_cmnd.replace('(label_window)', str(label_window))
    
    logger.debug('Creating labels...')
    
    conn, cur = create_pgconn()
    cur.execute(sql_cmnd)
    conn.commit()
    conn.close()

    logger.debug('Done creating labels. Connection committed')

    return

def train_test_model(learner, X_train, y_train, X_test, experiment, split_index, learner_index, model_index, normalize=False):
    '''
        Fits a learner to a train_df, and returns the scores of the test_df.
        Stores the model (experiment, split_index, learner_index) in the database
    '''
    
    X_test_df = X_test
    
    if normalize:
        logger.debug(f"Normalize flag TRUE. Normalizing data")
        scaler = normalize_data(X_train)
        X_train = scaler.transform(X_train)
        X_test = scaler.transform(X_test_df)
       
    logger.debug(f"SPLIT {split_index}, MODEL {model_index}. Training learner {learner}")
    t0 = datetime.datetime.now()
    learner.fit(X = X_train, y = y_train)
    train_time = round(((datetime.datetime.now() - t0).total_seconds())/60.0, 3)
    
    logger.debug(f"SPLIT {split_index}, MODEL {model_index}. Testing learner {learner}.")
    t0 = datetime.datetime.now()
    scores = learner.predict_proba(X = X_test)[:,1]
    scores_with_index = list(zip(list(X_test_df.index), scores))

    test_time = round(((datetime.datetime.now() - t0).total_seconds())/60.0, 3)
    
    conn, cur = create_pgconn()
    logger.debug(f"SPLIT {split_index}, MODEL {model_index}. Saving learner {learner} into database")
    cur.execute('set role el_salvador_mined_education_write;')
    cur.execute(f"""INSERT INTO results.models
            (
            model,
            experiment,
            learner,
            split,
            total_training_time,
            total_testing_time
            )
        VALUES
            (
            '{model_index}',
            '{experiment}',
            '{learner_index}',
            '{split_index}',
            '{train_time}',
            '{test_time}'
            )"""
                   )
    conn.commit()
    conn.close()
    logger.debug('Connection committed')
    
    return scores_with_index


def evaluate(labels, scores_with_index):
    """
        Evaluate the results from ML Model
    """
    scores = np.array([score[1] for score in scores_with_index])
    prec_k = precision_n(labels, scores) # np array, ordered by 1 - 100
    rec_k = recall_n(labels, scores) # np array, ordered by 1 - 100
    auc_score = auc(labels, scores)
    count = collections.Counter(labels)
    logger.debug('Count labels:', count)
    metrics = {'prec_k':prec_k, 'rec_k':rec_k, 'auc':auc_score,
               'real_pos': count[1], 'real_neg': count[0], 'total_pop': len(labels)}
    logger.debug(f"""Precision: {prec_k},
                    \nRecall: {rec_k},
                    \nAUC: {auc_score},
                    \nReal Positives: {count[1]},
                    \nReal Negatives: {count[0]},
                    \nTotal Population: {len(labels)}""")
    return metrics
