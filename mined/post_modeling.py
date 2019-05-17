# -*- coding: utf-8 -*-

"""
post_modeling.py contains helpful functions that creates plots (time stability and heat map of features) for selecting the best model. Also it prints the list of schools once a model is selected
"""

# Import statements

import pandas as pd
import numpy as np
import os
import os.path
import json
import matplotlib
matplotlib.use('agg')
import matplotlib.pyplot as plt
import importlib
from sklearn.metrics import precision_score, recall_score, roc_auc_score, precision_recall_curve, roc_curve
from sklearn.model_selection import ParameterGrid
from sklearn.preprocessing import StandardScaler
import datetime 

from mined.utils import create_pgconn, PLOTS_DIR, logger, SQL_DIR

from sklearn.externals.six import StringIO  
from IPython.display import Image  
from sklearn.tree import export_graphviz
import pydotplus
import seaborn as sns

import plotnine
from plotnine import *
from matplotlib import figure

from mined.utils import create_pgconn, PLOTS_DIR

conn, cur = create_pgconn()
cur.execute('set role el_salvador_mined_education_write')


# This function evaluates temporal stability of models
def eval_models(name, experiment_list, model_list, metric, all_models, output_type):
    
    if metric == 'auc':
        models = pd.read_sql(f"""select model, experiment, learner, algorithm, hyperparameters, auc as metric, extract(year from lower(test_span))::varchar as year from results.dashboard""", conn)   
    else:
        k = int(metric.split("_",1)[1])
        models = pd.read_sql(f"""select model, experiment, learner, algorithm, hyperparameters, precision_k[{k+1}] as metric, extract(year from lower(test_span))::varchar as year from results.dashboard""", conn)
    if all_models == False:
        models_df = pd.DataFrame({'experiment':experiment_list, 'model':model_list})
        models = pd.merge(models_df, models, how = 'left', on=['experiment', 'model'])

    models['idx'] = models['experiment'] + '_' + models['learner']              
    plot = ggplot(models, aes(x='year', y='metric', color = 'idx', group = 'idx')) + geom_point(size=1) + geom_line(size=1) + ylab(metric) + scale_y_continuous(limits=[0,1])

    if(output_type == 'save'):
        ggsave(plot, filename = name + '_' + metric, path = PLOTS_DIR)
    if(output_type == 'show'):
        plot
    else:
        plot

# This function receives an experiment and plot the heatmap of the feature importance of all the models within the experiment.
def heatmap_features(name, experiment, output_type):
    models = pd.read_sql(f"""select a.experiment, a.model, a.feature_name, a.score, b.algorithm, b.split, b.learner from results.feature_importances a left join results.dashboard b on a.experiment = b.experiment and a.model = b.model where a.experiment = '{experiment}' order by b.split, b.learner""", conn)        

    models['exp'] = 'split_' + models['split'].apply(str) + '_model_' + models['model']+ '_algorithm_' + models['algorithm']
    models_pt  = models.pivot_table(index = 'feature_name', columns = 'exp', values = 'score')
    models_pt = (models_pt - models_pt.min()) / (models_pt.max() - models_pt.min())
    
    # plot the map
    fig, ax = plt.subplots(figsize=(100,80))
    ax = sns.heatmap(models_pt, cmap="YlGnBu", xticklabels=True, yticklabels=True)
    plt.suptitle('Experiment ' + experiment)

    if(output_type == 'save'):
        plt.savefig(os.path.join(PLOTS_DIR, name + '_' + experiment + '.svg'), format = 'svg',  bbox_inches='tight')
    if(output_type == 'show'):
        plt.show()
    else:
        plt.show()

# This function receives a model dataframe an plots the heatmap of feature importance.
def heatmap_features_models(name, models_df, output_type):
    
    models = pd.read_sql(f"""select a.experiment, a.model, a.feature_name, a.score, b.algorithm, b.split, b.learner from results.feature_importances a left join results.dashboard b on a.experiment = b.experiment and a.model = b.model""", conn)


    models['exp'] = 'split_' + models['split'].apply(str) + '_model_' + models['model'] + '_algorithm_' + models['algorithm'] 
    models = pd.merge(models_df, models, how = 'left', on=['experiment', 'model'])
    models_pt = models.pivot_table(index = 'feature_name', columns = 'exp', values = 'score')
    models_pt = (models_pt - models_pt.min()) / (models_pt.max() - models_pt.min())

    # plot the map
    fig, ax = plt.subplots(figsize=(80,40))
    ax = sns.heatmap(models_pt, cmap="YlGnBu", xticklabels=True, yticklabels=True)
    plt.suptitle('Feature scores of ' + name)

    if(output_type == 'save'):
        plt.savefig(os.path.join(PLOTS_DIR, name, '.svg'), bbox_inches ='tight')
        if(output_type == 'show'):
            plt.show()
        else:
            plt.show()


# Print schools_list for interventions
def schools_list(name, experiment, model):
    query = open(SQL_DIR + 'schools_list.sql', 'r')
    df = pd.read_sql_query(query.read(), conn, params = {experiment, model})
    df.to_csv(PLOTS_DIR + experiment + '_' + model + '_schools_list.csv', encoding = 'utf-8')
    logger.info(f'School list is here: {PLOTS_DIR}')

