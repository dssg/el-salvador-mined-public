# coding: utf-8

import os

import s3fs

import click
import yaml
import sqlalchemy

from sqlalchemy.event import listens_for
from sqlalchemy.pool import Pool

import datetime

import triage
from triage.experiments import MultiCoreExperiment
from triage.experiments import SingleThreadedExperiment

import logging

logging_level = logging.DEBUG

logging.basicConfig(
    format="%(name)-30s  %(asctime)s %(levelname)10s %(process)6d  %(filename)-24s  %(lineno)4d: %(message)s",
    datefmt = "%d/%m/%Y %I:%M:%S %p",
    level=logging_level,
    handlers=[logging.StreamHandler(),
    ]
)


@click.command()
@click.option('--experiment-file', type=click.Path(),
              help="Triage's experiment configuration file")
@click.option('--output-path',
              type=click.Path(),
              help="Triage's output path (For storing matrices and trained models)")
@click.option('--replace/--no-replace',
              help="Triage will (or won't) replace all the matrices and models",
              default=True)  ## Default True so it matches the default behaviour of Triage
@click.option('--models/--no-models',
              help="Triage will (or won't) build models",
              default=True)
@click.option('--save-predictions/--no-save-predictions', 
              help = "Skip saving predictions to the database to save time",
              default=True )
def run_experiment(experiment_file, output_path, replace, models,  save_predictions):

    start_time = datetime.datetime.now()
    logging.info(f"Reading the file experiment configuration from {experiment_file}")

    # Load the experiment configuration file
    s3 = s3fs.S3FileSystem()
    with s3.open(experiment_file, 'rb') as f:
        experiment_config = yaml.load(f.read())

    host = os.environ['POSTGRES_HOST']
    user = os.environ['POSTGRES_USER']
    db = os.environ['POSTGRES_DB']
    password = os.environ['POSTGRES_PASSWORD']
    port = os.environ['POSTGRES_PORT']

    db_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    logging.info(f"Using the database: postgresql://{user}:XXXXX@{host}:{port}/{db}")

    try:
        n_processes = int(os.environ.get('NUMBER_OF_PROCESSES', 1))
    except ValueError:
        n_processes = 1
    try:
        n_db_processes = int(os.environ.get('NUMBER_OF_DB_PROCESSES', 6))
    except ValueError:
        n_db_processes = 6

    logging.info(f"The experiment will use {n_processes} cores in the host")

    logging.info(f"The output (matrices and models) of this experiment will be stored in {output_path}")

    logging.info(f"The experiment will utilize any preexisting matrix or model: {not replace}")

    logging.info(f"Creating experiment object")

    experiment = MultiCoreExperiment(
        n_processes=n_processes,
        n_db_processes=n_db_processes,
        config=experiment_config,
        #db_engine=sqlalchemy.create_engine(db_url),
        db_engine = triage.create_engine(db_url),
        project_path=output_path,
        replace=replace,
        save_predictions=save_predictions
    )

    logging.info(f"Experiment created: all the file permissions, and db connections are OK")


    logging.info(f"Validating the experiment")

    experiment.validate()

    logging.info("""
           The experiment configuration doesn't contain any obvious errors.
           Any error that occurs possibly is related to number of columns or collision in
           the column names, both due to PostgreSQL limitations.
    """)

    logging.debug(f"Experiment configuration: {experiment.config}")

    experiment_name = os.path.splitext(os.path.split(experiment_file)[1])[0]

    logging.info(f"Running the experiment: {experiment_name}")

    experiment.split_definitions
    experiment.generate_matrices()
    if models:
        experiment.train_and_test_models()

    end_time = datetime.datetime.now()

    logging.info(f"Experiment {experiment_file} completed in {end_time - start_time} seconds")

    logging.info("Done!")


if __name__ == '__main__':
    run_experiment()
