# Triage

## ETL

The El Salvador Ministry of Education (MINED) shared several data files in Excel spreadsheets that we used as the raw data for our analysis:
1. **Student Registration**: Registration data of all students attending public school. Contains student-level information including unique ID number, age, gender, number of family members, etc. Used to generate student-level features.
2. **Student Graduation**: Promotion status of students at the end of the course. Used to generate student-level features and label.
3. **School Census**: Public data containing information about schools (location, number teachers, ownership, etc.). Used to generate school-level features.
4. **Student Census**: Public data with student information (including school attended) but without student ID field necessary to join with other student information. Used to generate school-level features.
5. **Program Monitoring Survey**: Annual survey completed by principals, includes information on social challenges in community. Used to generate school-level features.
6. **Monthly School Enrollment Statistics**: total number of students enrolled monthly by school 
7. **Social Programs Survey**: indicates number of students participating in social programs by gender by school
8. **Risk Index**: underlying data used to create regional risk index
9. **Multipurpose Household Survey**: demographic information at household level 
10. **Social Programs Budget**: budget of social programs implemented
11. **Violence**: yearly crime statitics by type of crime and administrative division
12. **PAES**: standardized test (Learning and Skills Test for Graduates of Secondary Education) scores by school

Only files 1-2 were used in the final models discussed for this project. We created features using files 3-5 but were not able to incorporate those additional features in this modeling stage, and hope to do so in subsequent stages of this project. Files 6-10 were not used due to insufficient date coverage. Files 11-12 were not used due to missing data.

The 2018 DSSG Fellows completed the following etl steps to transform the raw data into the cleaned tables in Postgres:

1. [Raw file to CSV](https://github.com/dssg/El_Salvador_mined_education/blob/master/ETL/mined/ETL/to_clean_csv.py): convert raw files stored on the server into preprocessed CSV files
2. [CSV to Postgres](https://github.com/dssg/El_Salvador_mined_education/blob/master/ETL/mined/ETL/to_db_raw.py): Copy CSV files into separate tables in raw schema of Postgres database.
3. Manual Column Mapping: the fellow team to manually mapped the columns from each file into usable columns names.
4. [Join tables](https://github.com/dssg/El_Salvador_mined_education/blob/master/ETL/mined/ETL/to_db_preproc.py): the fellows used column mappings to join tables of the same type (eg. student tables from different years).
5. [Create Cleaned Table](https://github.com/dssg/El_Salvador_mined_education/blob/master/ETL/mined/ETL/to_db_clean.py): the tables created in step 4 are cleaned and relevant data types converted. The resulting tables are stored in the cleaned schema.

DSaPP then used these cleaned tables to create the final semantic schema that is used for modeling. The semantic schema contains the following tables:
- `Entity Student`: Contains the unique student IDs for the students in our population
- `Events Student`: Contains records of each enrollment for a given student in a given year, along with information about that student (eg. age, gender, etc.). Obserevations are at the student-year-school level. If a student was enrolled in multiple schools in a given year (eg. if they transferred schools), they would have multiple rows for that year.
- `Events School`: Contains information about each public school in El Salvador in a given year, such as number of teachers, owner, etc.

Each time an experiment is run using triage, it outputs a trained model object and data matrix for each classifier-hyperparameter combination specified in the grid config (see below for more details). These model and matrix objects are stored using Amazon Web Serivces S3.

## Experiment Configuration File

### What is the temporal structure of your problem? (`temporal_config`) 

This section defines configuration for temporal cross-validation including feature start and end date, label start and end date, frequency of model update, training, and testing as-of-dates, and timespan of training and testing label. In our case, we set the model update frequency, training/test label timespan and training/test label frequency to 1 year each. We do this because MINED is interested in generating predictions of students who will drop out of school in the beginning of the calendar year (at which point the school enrollment for that school year will be finalized) to inform their annual allocation of social programs funding later in the year. Therefore, it makes sense in our case to generate annual predictions, and predict for one year ahead. This yields 6 temporal splits per the diagram below:

<p align="center">
  <img src="https://github.com/dssg/El_Salvador_mined_education/blob/master/images/temporalcrossvalidation.png">
</p>

### Who are the entities of interest? (`cohort_config`)

Contains a SQL query that defines the cohort for a given as of date. In our case, the cohort is all unique students enrolled in a public school in El Salvador on the given as of date.

### What do we want to predict? (`label_config`) 

Contains a SQL query that define the label for each student in the cohort. In our case, students who do not appear in the data set in the subsequent year and did not graduate in the current year are given a label of 1 (dropped out of school) and all other students are given a label of 0. 

### What features do we want? (`feature_aggregations`)
Defines feature groups (`prefix` in config file) and individual features for experiment. For each feature group, a SQL query is provided that defines the `from obj`, a SQL object from which the individual features are calculated. For each individual feature, the aggregation (eg. max, avg) and strategy for imputing missing values is defined. For each feature group, a list of time intervals for aggregation is defined. These time intervals define how far back from the as-of-date we look to calculate features (eg. whether student reported an illness in the past 3 years). See below for a feature example.

### Which models do you you want to train? (`grid_config`) 
Defines the set of classifiers and hyperparameters that the experiment will search over.

### Which metrics do you want to use to evaluate model performane? (`scoring`)
Defines the set of metrics that will be calculated for training and testing. Includes set of metrics (eg. 'precision@') and thresholds (eg. 10%). 

### Feature Example

Below configuration file feature entry would create the following features:

- `rural_entity_id_1y_rural_bool_max`: whether student attended any rural school in past 1 year
- `rural_entity_id_1y_rural_bool_avg`: proportion of schools attended by past 1 year that are rural
- `rural_entity_id_3y_rural_bool_max`: whether student attended any rural school in past 3 years
- `rural_entity_id_3y_rural_bool_avg`: proportion of schools attended by stident in past 3 years that are rural
- `rural_entity_id_5y_rural_bool_max`: whether student attended any rural school in past 5 years
- `rural_entity_id_5y_rural_bool_avg`: proportion of schools attended by student in past 5 years that are rural
- `rural_entity_id_all_rural_bool_max`: whether student attended any rural school in all years
- `rural_entity_id_all_rural_bool_avg`: proportion of schools attended by student in all years that are rural

```
    prefix: 'rural'
    from_obj: |
      (select student::int as entity_id, event_date, school, code, rural from semantic.events_student left join       cleaned."1_cleaned" on events_student.school = cleaned."1_cleaned".code 
      and EXTRACT(year from events_student.event_date) = EXTRACT(year from lower(cleaned."1_cleaned".year_range)) where length(student) > 3 and length(student) < 9) as rural

    knowledge_date_column: 'event_date'

    aggregates_imputation:
         all:
          type: 'constant'
          value: 0  

    aggregates:
      - # rural_boolean
        quantity:
          rural_bool: "case when rural = 'rural' then 1 else 0 end"
        metrics:
          - 'avg'
          - 'max'

    intervals: ['1y', '3y', '5y', 'all']
```

## Index of Experiments
- `readme_config.yaml`: Configuration file that could be run to generate the final three models produced for this project.
- `dropout_01.yaml`: Main modeling run including student features and a large grid config. Includes the following feature groups: over age, repeater, rural, events, dropout, gender, commute, illness, and family
- `dropout_02.yaml`: Configuration file with both student and school features and a smaller grid config for testing. This experiment could not be run within the current memory constraints. Includes the following feature groups: over age, repeater, rural, events, dropout, gender, commute, illness, family, school issues, school aggregates, school categoricals, and geographic .
- `dropouts_mined.yaml`: Baseline models including only the set feature groups identified as most important by MINED at the outset of this project: over age, repeater, and rural.
- `dropouts_predictions.yaml`: Configuration file used to generate features for producing out of sample predictions. The temporal_config of this configuration file produces one feature temporal split with the as-of-date of 2018-01-01. 
 




