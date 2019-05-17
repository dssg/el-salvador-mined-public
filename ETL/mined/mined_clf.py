from sklearn.base import TransformerMixin
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class MinedClassifier(TransformerMixin):
    
    def __init__(self):
        super()
       
    def predict(df):
        '''
            Implementation of current expert knowledge at MINED for predicting dropout
            
            :param df: student matrix
            :type df: data_frame
            
            :return: prediction
            :rtype: data_frame
        '''
        pass
    
    def fit(df):
        pass