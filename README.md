# Reducing Early School Dropout Rates in El Salvador

![DSSG Logo](http://dssg.uchicago.edu/wp-content/uploads/2017/06/dssglogounbranded-4-e1513053611365.png)

### Partner: Ministry of Education - Vice Ministry of Science and Technology, El Salvador
![VM of Research in Science](http://www.cienciaytecnologia.edu.sv/templates/vcyt/img/logo.jpg)

Salvadoran students across all levels of mandatory education are dropping out at an alarming rate. This leads to many negative educational, economic, social, and political consequences for the country. The Ministry of Education (MINED) would like an early warning system that helps identify students who are likely to drop out or schools that are likely to have retention issues. MINED will use the predictions to provide support to the students and schools that need it. DSaPP will receive data covering the entire country, going about ten years back, at an individual level, for teachers, students, schools, demographics, and crime.

## Setup

- ETL scripts are a combination of python scripts (run using Python 3.6) and bash scripts
- SQL scripts for inserting raw files and creating features are written for PostgresSQL 9.5.10
- Model training pipeline is run using Python 3.6
- The `mined` package was created on Ubuntu v. 18.04 on AWS servers

## What to expect in this repo
- ETL scripts to transform raw files in xlsx/csv/sav format into PostgresSQL database
- Scripts to retrain model and generate recall, precision, auc metrics
- EDA Jupyter notebooks used for pre and post model analysis

## Installing MINED package
Prereq: ensure environment has Python 3.6 and Postgres 9.5.10.
1. Clone this repo
2. `pip install -r requirements.txt`
    - A list of necessary packages should be downloaded onto your machine.
    - Estimated time: 5 minutes
3. `cd El_Salvador_mined_education`
4. `pip install .`
    - This will install the `mined` package
    - Validate the package is installed correctly by running: `mined --help`. Your expected output should be: <screenshot of output>
    
## Running the Pipeline
Prior to collaboring with DSSG, data consume by the MINED were intended for reporting purposes only. In addition, files of the same type had inconsistent column headers across years. Our pipeline transforms these files to be consumed by an analytical database and used for modeling. It does so by completing the following steps:

[ETL](https://github.com/dssg/El_Salvador_mined_education/tree/master/mined/ETL)
1) [Raw file to CSV:](/mined/ETL/to_clean_csv.py) convert raw files stored on the server into comprehensible csvs
2) [CSV to Postgres:](/mined/ETL/to_db_raw.py) CSVs  inserts them into Postgres. Each file on the server is stored as its own table in the `raw` schema (1:1, file to table).
3) **Manual Column Mapping:** this step requires someone on the team to manually map the columns from each file into usable columns into a csv. This csv should be stored on the server.
4) [Join tables:](/mined/ETL/to_db_preproc.py) the column mapping csv (completed in step 2) is used to join tables of the same type - this eliminates 1:1 file to table ratio.
5) [Create Cleaned Table:](/mined/ETL/to_db_clean.py) this step completes some preliminary data cleaning as well as data type conversions. The result are tables in our `cleaned` schema.
6) [Create Semantic Entity Tables:](/mined/ETL/to_staging.py)Tables of semantic understanding are created at this stage. During the fellowship the two entities we focused on were *students* and *schools*. For every student in our data from 2008 - 2018, static information is kept in the `students` table. For every school in our data from 2008 - 2018, static information is kept in the `schools` table. *students* data that changed over time is placed in our `events` table

[Run Models](https://github.com/dssg/El_Salvador_mined_education/tree/master/mined)

Running different models and subsequent experiments are done in the `mined` package. Experiments are defined in [config files](/experiments/). This is a sample of a [completed experiment config](/experiments/extratrees.yaml), this is an [empty template](/experiments/example.yaml).

To run an experiment:
    `mined --config-file </path/to/config>`

## Issues
Please use the [issue tracker](https://github.com/dssg/El_Salvador_mined_education/issues/new) to report issues or suggestions.

## Contributors
Yago del Valle-Inclán Redondo, Dhany Tjiptarto, Ana Valdivia, Adolfo De Unanue (Technical Mentor), Mirian Lima (Project Manager)
