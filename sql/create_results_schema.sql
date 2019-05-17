set role el_salvador_mined_education_write;

drop table if exists results.experiments;
create table if not exists results.experiments (
    experiment varchar, -- PK
    git_hash varchar,
    config jsonb, -- retrieved from run.py
    run_date timestamp,
    feature text[],
    seed int
);


drop table if exists results.models;
create table if not exists results.models  (
    model varchar, -- PK
    experiment varchar, -- FK to results.experiments
    learner varchar, -- FK to results.learner
    split serial, -- FK to results.splits
    total_training_time varchar,
    total_testing_time varchar,
    total_pr_graph_time varchar,
    model_comment text,
    seed int
);


drop table if exists results.learners;
create table if not exists (
    learner varchar,
    algorithm varchar,
    experiment varchar, -- FK to results.experiments
    hyperparameters jsonb -- retrieved from run.py
);


drop table if exists results.splits; -- 
create table if not exists results.splits(
    split serial, -- PK
    experiment varchar, -- FK to results.experiments
    train_span daterange,
    test_span daterange,
    train_label_span daterange,
    test_label_span daterange
);


drop table if exists results.matrices;
create table if not exists results.matrices  (
    experiment varchar, -- FK to results.experiments
    matrix varchar, -- actual matrix stored in /matrices
    split serial -- FK to results.splits
);


drop table if exists results.feature_importances;
create table if not exists results.feature_importances  (
    model varchar, -- FK to results.models
    feature_name text,
    score decimal
);

drop table if exists results.feature_importances2;
create table if not exists results.feature_importances2  (
    experiment varchar,
    model varchar, -- FK to results.models
    feature_name text,
    score decimal
);


drop table if exists results.predictions;
create table if not exists results.predictions  (
    experiment varchar, -- FK to results.experiments
    model varchar, -- PK to results.models
    student varchar, -- entity of interest
    year varchar, -- MUST CHANGE, year is converted to python datetime
    score decimal, -- normalized across models
    prediction_date date
);

--label varchar, -- dropped out or not



drop table if exists results.metrics;
create table if not exists results.metrics  (
    experiment varchar,
    model varchar, -- PK to results.models
    precision_k decimal[], -- ORDERED array ranging from 1%-100% of population
    recall_k decimal[], -- ORDERED array ranging from 1%-100% of population
    auc decimal,
    total_population int,
    total_real_positive int,
    total_real_negative int
);
