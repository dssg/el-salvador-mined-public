from triage.component.architect.feature_generators import FeatureGenerator
from triage.util.db import create_engine
import logging
import yaml
import os


host = os.environ['POSTGRES_HOST']
user = os.environ['POSTGRES_USER']
db = os.environ['POSTGRES_DB']
password = os.environ['POSTGRES_PASSWORD']
port = os.environ['POSTGRES_PORT']

db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

logging.info(f"Using the database: postgresql://{user}:XXXXX@{host}:{port}/{db}")


logging.basicConfig(level=logging.INFO)

# create a db_engine 
db_url = 'your db url here'
db_engine = create_engine(db_url)

feature_config = [{
    'prefix': 'aprefix',
    'aggregates': [
        {
        'quantity': 'quantity_one',
        'metrics': ['sum', 'count'],
    ],
    'categoricals': [
        {
            'column': 'cat_one',
            'choices': ['good', 'bad'],
            'metrics': ['sum']
        },
    ],
    'groups': ['entity_id', 'zip_code'],
    'intervals': ['all'],
    'knowledge_date_column': 'knowledge_date',
    'from_obj': 'data'
}]

FeatureGenerator(db_engine, 'features_test').create_features_before_imputation(
    feature_aggregation_config=feature_config,
    feature_dates=['2016-01-01']
)