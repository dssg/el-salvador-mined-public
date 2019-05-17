# -*- coding: utf-8 -*-

"""
ml.py contains helpful functions for machine learning tasks.
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
from sklearn.preprocessing import MinMaxScaler
import datetime 

from mined.utils import create_pgconn, PLOTS_DIR, logger

from sklearn.externals.six import StringIO  
from IPython.display import Image  
from sklearn.tree import export_graphviz
import pydotplus


# Helper functions

def dummifier(training_df, dummy_na=True):
    dummy_df = pd.get_dummies(training_df, dummy_na=dummy_na)
    return dummy_df

def normalize_data(training_df):
    scaler = MinMaxScaler().fit(training_df)
    return scaler

def extract_learners(configs, experiment):
    conn, cur = create_pgconn()
    clfs = []
    learners = configs['learners']

    idx = 0
    cur.execute("set role el_salvador_mined_education_write;")
    for learner in learners:
        hyperparameters = ParameterGrid(learner['hyperparameters'])
        for hyperparameter in hyperparameters:
            module_name, class_name = learner['algorithm'].rsplit('.', 1)
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            clf = cls(**hyperparameter)
            clfs.append(clf)
            hyperparameters_json = json.dumps(hyperparameter, indent=4, sort_keys=True, default=str)
            cur.execute(f"""INSERT INTO results.learners (
                learner,
                algorithm,
                experiment,
                hyperparameters
            )
                VALUES
                (
                '{idx}',
                '{learner['algorithm']}',
                '{experiment}',
                '{hyperparameters_json}'
                )"""

            )
            idx += 1
    conn.commit()
    conn.close()
    return clfs


def joint_sort_descending(l1, l2):
    # l1 and l2 have to be numpy array
    idx = np.argsort(l1)[::-1]
    return l1[idx], l2[idx]

def generate_binary_at_k(y_scores, k):
    cutoff_index = int(len(y_scores) * (k / 100.0))
    predictions_binary = [1 if x < cutoff_index else 0 for x in range(len(y_scores))]
    return predictions_binary

def precision_at_k(y_true, y_scores, k):
    y_scores_sorted, y_true_sorted = joint_sort_descending(np.array(y_scores), np.array(y_true))
    preds_at_k = generate_binary_at_k(y_scores_sorted, k)
    precision = precision_score(y_true_sorted, preds_at_k)
    return precision

def recall_at_k(y_true, y_scores, k):
    y_scores_sorted, y_true_sorted = joint_sort_descending(np.array(y_scores), np.array(y_true))
    preds_at_k = generate_binary_at_k(y_scores_sorted, k)
    recall = recall_score(y_true_sorted, preds_at_k)
    return recall

def precision_n(y_true, y_scores):
    precisions = []
    for i in range(101):
        prec = precision_at_k(y_true, y_scores, i)
        precisions.append(prec)
    return precisions

def recall_n(y_true, y_scores):
    recalls = []
    for i in range(101):
        rec = recall_at_k(y_true, y_scores, i)
        recalls.append(rec)
    return recalls


def auc(y_true, y_scores):
    return roc_auc_score(y_true, y_scores)


def plot_roc(name, probs_with_index, true, output_type):
    split_name = name.split('_')
    probs = np.array([probs[1] for probs in probs_with_index])
    fpr, tpr, thresholds = roc_curve(true, probs, pos_label=1)
    roc_auc = auc(true, probs)
    plt.clf()
    plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.xlim([0.0, 1.05])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title(f"Experiment: {split_name[0]}\nModel: {split_name[1]}")
    plt.legend(loc="lower right")
    
    if (output_type == 'save'):
        plt.savefig(os.path.join(PLOTS_DIR, name))
        plt.close()
    elif (output_type == 'show'):
        plt.show()
    else:
        plt.show()


def plot_precision_recall_n(name, experiment, precisionk, recallk, output_type):
    t0 = datetime.datetime.now()
    split_name = name.split('_')
    y_axis = np.arange(0, 101, 1)
    
    plt.clf()
    fig, ax1 = plt.subplots()
    ax1.plot(y_axis, precisionk, 'b') # x axis is just 0 - 1
    ax1.set_xlabel('percent of population')
    ax1.set_ylabel('precision', color='b')
    plt.suptitle(f"Experiment: {split_name[0]}\nModel: {split_name[1]}")
    ax2 = ax1.twinx()
    ax2.plot(y_axis, recallk, 'r') # x axis is just 0 - 1
    ax2.set_ylabel('recall', color='r')
    ax1.set_ylim([0,1])
    ax2.set_ylim([0,1])
    ax2.set_xlim([0,101])

    if (output_type == 'save'):
        plt.savefig(os.path.join(PLOTS_DIR, name))
        plt.close()
    elif (output_type == 'show'):
        plt.show()
    else:
        plt.show()
        
    t1 = datetime.datetime.now()
    total_time = round(((datetime.datetime.now() - t0).total_seconds())/60.0, 3)
    conn, cursor = create_pgconn()
    cursor.execute("set role el_salvador_mined_education_write;")
    cursor.execute(f"""UPDATE results.models SET total_pr_graph_time = '{total_time}' WHERE experiment = '{experiment}'""")
    conn.commit()
    conn.close()
    logger.debug(f'Total time PR graph took: {total_time}')

        
def plot_scores(name, pos_probs_with_index, output_type):
    pos_probs = np.array([probs[1] for probs in pos_probs_with_index])
    neg_probs = np.array([1 - score for score in np.nditer(pos_probs)])
    logger.debug(f"NEG_PROBS: {neg_probs}")
    fig, axs = plt.subplots(1, 2, sharey=True)
    axs[0].hist(pos_probs, bins=10)
    axs[1].hist(neg_probs, bins=10, color='r')
    axs[0].set_title('Probability of Dropping Out', color='b')
    axs[1].set_title('Probability for Not Dropping Out', color='r')
    axs[0].set_xlabel('Scores')
    axs[1].set_xlabel('Scores')
    axs[0].set_ylabel('Count')
    axs[1].set_ylabel('Count')
    axs[0].set_xlim([0,1])
    axs[1].set_xlim([0,1])
    plt.subplots_adjust(wspace=1)
    
    if (output_type == 'save'):
        plt.savefig(os.path.join(PLOTS_DIR, name))
        plt.close()
    elif (output_type == 'show'):
        plt.show()
    else:
        plt.show()

        
def plot_feature_importance_rf(name, df, predictor, mean_importance_array, std_importance_array, indices, output_type):
    # Print the feature ranking
    logger.debug("Ranking.")
#     for f in range(len(indices)):
#         logger.debug(f"{f + 1}. feature {predictor[indices[f]]} {indices[f]} ({mean_importance_array[indices[f]]})")

    sorted_predictors = [x for (y,x) in sorted(zip(mean_importance_array,predictor),reverse=True)]    

    # Plot the feature importances of the forest
    plt.figure()
    plt.title(name)
    plt.bar(range(len(indices)), mean_importance_array[indices],
           color="m", yerr=std_importance_array[indices], align="center")
    plt.xticks(range(len(indices)), sorted_predictors,rotation=90)
    plt.xlim([-1, len(indices)])
    plt.tight_layout()
    
    if (output_type == 'save'):
        plt.savefig(os.path.join(PLOTS_DIR, name))
        plt.close()
    elif (output_type == 'show'):
        plt.show()
        plt.close()
    else:
        plt.show()
        plt.close()

        
def plot_feature_importance(name, df, predictor, mean_importance_array, indices, output_type):
    """
        Plots features in ranking order of importance

        :param name: A string formatted as: 'experimentnumber_FEATURES.png'
        :type name: string
        :param df: test dataframe
        :type df: datarame
        :param predictor: columns of df indicating the feature names
        :type predictor: column array
        :param mean_importance_array: unsorted array of feature importance values of features. Order of values are in order of where the columns appear in the test_df
        :type mean_importance_array: numpy array
        :param indices: numpy array with indicies indicated which how to sort importance values to achieve descending importance values
        :type indices: numpy array
        :param output_type: Whether to save or show plot
        :type output_type: string
        
        :return: None - saved plot into PLOT_DIR
    """
    
    
    logger.debug("Ranking.")
#     for f in range(len(indices)):
#         logger.debug(f"{f + 1}. feature {predictor[indices[f]]} {indices[f]} ({mean_importance_array[indices[f]]})")
        
    sorted_predictors = [x for (y,x) in sorted(zip(mean_importance_array, predictor),reverse=True)]    
    plt.figure()
    plt.title(name)
    plt.bar(range(len(indices)), mean_importance_array[indices], color="m", align="center")
    plt.xticks(range(len(indices)), sorted_predictors, rotation=90)
    plt.xlim([-1, len(indices)])
    plt.tight_layout()
                   
    if (output_type == 'save'):
        plt.savefig(os.path.join(PLOTS_DIR, name))
        plt.close()
    elif (output_type == 'show'):
        plt.show()
        plt.close()
    else:
        plt.show()
        plt.close()


def calculate_feature_importance_rf(clf):
    importances = clf.feature_importances_
    std_importances = np.std([tree.feature_importances_ for tree in clf.estimators_],
            axis=0)
    indices = np.argsort(importances)[::-1]
    return importances, std_importances, indices

def calculate_feature_importance(clf):
    importances = clf.feature_importances_
    indices = np.argsort(importances)[::-1]
    return importances, indices


def calculate_feature_importance_lr(clf):
    importances = clf.coef_[0]
    importances_std = np.std(importances, axis=0)*clf.coef_
    indices = np.argsort(importances)[::-1]
    return importances, importances_std, indices


# plot decision tree
def plot_dt(learner, features, name, output_type):
    dot_data = StringIO()
    export_graphviz(learner, file=dot_data,
            filled=True, rounded=True,
            special_characters=True,
            features_name = features, 
            class_names = ('no dropout', 'dropout'))
    graph = pydotplus.graph_from_dot_data(dot_data.getvalue())
    if(output_type == 'save'):
        graph.write_png(os.path.join(PLOTS_DIR, name, ".png"))
    elif(output_type == 'show'):
        Image(graph.create_png())
    else:
        Image(graph.create_png())
