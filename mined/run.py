# -*- coding: utf-8 -*-

import yaml
import pandas as pd
import numpy as np
import os.path
import gc
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC

import click

from .pipeline import create_features, create_labels, calculate_splits, get_matrices, evaluate, create_experiment, train_test_model
from .ml import extract_learners, plot_roc, plot_precision_recall_n, plot_scores, calculate_feature_importance_lr, plot_feature_importance, calculate_feature_importance_rf, plot_feature_importance_rf, calculate_feature_importance, plot_dt, dummifier
from .utils import logger, store_predictions, store_evaluations, store_feature_importances, PLOTS_DIR

@click.command()
@click.option('--config-file', help='Configuration file name',
              required=True, type=click.Path())
@click.option('--features', is_flag=True, help='If present it will create the features table specified in the config file')
@click.option('--labels', is_flag=True, help='If present it will create the labels table specified in the config file')
@click.option('--debug', is_flag=True, help='If present, run on subset of data to test pipeline')
@click.option('--rerun', help='Rerun pipeline on `experiment,split`', type=click.STRING)
def run(config_file, features, labels, debug, rerun):

    logger.debug(config_file)
    with open(config_file, 'r') as f:
        configs = yaml.load(f)

    if debug:
        experiment = 'testing_pipeline'
        splits = calculate_splits(configs, experiment)
        #splits = splits.loc[:0, splits.columns]
    else:
        experiment, configs = create_experiment(config_file)
        splits = calculate_splits(configs, experiment)
    logger.debug(f'Running experiment: {experiment}')

    if rerun:
        experiment, split_num, model_start = rerun.split(',')
        logger.info(f'Running on EXPERIMENT: {experiment}, SPLIT: {split_num}')
        splits = calculate_splits(configs, experiment, rerun=True)
        splits = splits.loc[split_num:, splits.columns]
        if model_start:
            model_index = int(model_start)
            model_start=None
        else:
            model_index = 0
    if features:
        logger.info("Creating features")
        create_features(configs)
    if labels:
        logger.info("Creating labels")
        create_labels(configs)

    learners = extract_learners(configs, experiment)
    logger.info(f'LEARNERS: {learners}')
    
    if not rerun:
        model_index = 0
    
    for split_index, split in splits.iterrows():
        logger.debug(f"""Using split train span: {split['train_span'].lower.strftime('%Y/%m/%d')} - {split['train_span'].upper.strftime('%Y/%m/%d')},
                \nTest span: {split['test_span'].lower.strftime('%Y/%m/%d')} - {split['test_span'].upper.strftime('%Y/%m/%d')},
                \nTrain label span: {split['train_label_span'].lower.strftime('%Y/%m/%d')} - {split['train_label_span'].upper.strftime('%Y/%m/%d')},
                \nTest label span: {split['test_label_span'].lower.strftime('%Y/%m/%d')} - {split['test_label_span'].upper.strftime('%Y/%m/%d')}""")
        
        if debug:
            X_train, y_train, X_test, y_test = get_matrices(split, configs, experiment, split_index, debug=True)
        else:
            X_train, y_train, X_test, y_test = get_matrices(split, configs, experiment, split_index)
        
        # Convert all features in X to dummies
        #logger.debug(f'# columns before dummifying: {len(X_train.columns)}')
        #X_train = dummifier(X_train)
        #X_test = dummifier(X_test)
        #logger.debug(f'# columns after dummifying: {len(X_train.columns)}')
        
        for learner_index, learner in enumerate(learners):
            if isinstance(learner, (LogisticRegression, SVC)):
                scores_with_index = train_test_model(learner, X_train, y_train, X_test, experiment, split_index, learner_index, model_index, normalize=True)
            else:
                scores_with_index = train_test_model(learner, X_train, y_train, X_test, experiment, split_index, learner_index, model_index)

            logger.debug(f"SPLIT {split_index}, MODEL {model_index}. Storing predictions from learner {learner}")
            store_predictions(scores_with_index, experiment, model_index)
            logger.debug(f"SPLIT {split_index}, MODEL {model_index}. Calculating evaluation metrics for learner {learner}")
            metrics = evaluate(y_test, scores_with_index)
            
            logger.debug(f"SPLIT {split_index}, MODEL {model_index}. Storing evaluations metrics from learner {learner}")
            store_evaluations(metrics, experiment, model_index)
            
            logger.debug(f"Saving ROC graphs")
            root_name = f"{experiment}_{model_index}"
            name_roc = root_name + "_ROC.png"
            name_pr = root_name + "_PR.png"
            name_scores = root_name + "_PROBAS.png"
            name_features = root_name + "_FEATURES.png"
            plot_roc(name_roc, scores_with_index, y_test, 'save')
            
            logger.debug('Saving Precision/Recall graphs')
            plot_precision_recall_n(name_pr, experiment, metrics['prec_k'], metrics['rec_k'], 'save')

            logger.debug('Saving Proba score graphs')
            plot_scores(name_scores, scores_with_index, 'save')
                       
            logger.debug(f"SPLIT {split_index}, MODEL {model_index}. Calculating feature importance.")
            
            if isinstance(learner, (LogisticRegression, SVC)):
                importance, std_importance, indices = calculate_feature_importance_lr(learner)
                logger.debug(f"Feature importances: {importance}")
                logger.debug("Creating and storing feature importances plots")
                plot_feature_importance(name_features, X_test, X_test.columns, importance, indices, 'save')
            elif isinstance(learner, RandomForestClassifier):
                importance, std_importance, indices = calculate_feature_importance_rf(learner)
                logger.debug(f"Feature importances: {importance}")
                logger.debug("Creating and storing feature importances plots")
                plot_feature_importance_rf(name_features, X_test, X_test.columns, importance, std_importance, indices, 'save')
            else:
                importance, indices = calculate_feature_importance(learner)
                logger.debug(f"Feature importances: {importance}")
                logger.debug("Creating and storing feature importances plots")
                plot_feature_importance(name_features, X_test, X_test.columns, importance, indices, 'save')
            logger.debug("Storing feature importances")
            store_feature_importances(experiment, model_index, X_test.columns, importance, indices)
            gc.collect() 
            model_index += 1
        del X_train
        del y_train
        del X_test
        del y_test
        gc.collect()
