set role el_salvador_mined_education_write;

drop table if exists staging.labels_debug;
create table staging.labels_debug as
select *
from staging.labels
tablesample SYSTEM (3);
create index labelsdebug_year_student_idx on staging.labels_debug(year_range, student);

drop table if exists results.features_onehot_debug;
create table results.features_onehot_debug as
select *
from results.features_onehot
where student in (select distinct student
from staging.labels_debug); -- results in 5mil rows
create index onehotdebug_year_student_idx on results.features_onehot_debug(year_range, student);

drop table if exists results.features_timeaggregates_debug;
create table results.features_timeaggregates_debug as
select *
from results.features_timeaggregates
where student in (select distinct student
from staging.labels_debug); -- results in 5mil rows
create index taggdebug_year_student_idx on results.features_timeaggregates_debug(year_range, student);

drop table if exists results.features_timeaggregates_small_debug;
create table results.features_timeaggregates_small_debug as
select *
from results.features_timeaggregates_small
where student in (select distinct student
from staging.labels_debug); -- results in 5mil rows
create index taggsmalldebug_year_student_idx on results.features_timeaggregates_small_debug(year_range, student);

drop table if exists results.features_aggregate_debug;
create table results.features_aggregate_debug as
select *
from results.features_aggregate
where student in (select distinct student
from staging.labels_debug); -- results in 5mil rows
create index agg_debug_year_student_idx on results.features_aggregate_debug(year_range, student);

drop table if exists results.features_imputed_debug;
create table results.features_imputed_debug as
select *
from results.features_imputed
where student in (select distinct student
from staging.labels_debug); -- results in 5mil rows
create index timpdebug_year_student_idx on results.features_imputed_debug(year_range, student);
