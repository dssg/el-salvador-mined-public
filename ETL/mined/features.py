# -*- coding: utf-8 -*-

from mined.utils import SQL_DIR, create_pgconn, logger
import re


def feature_list_from_SQL(file_name):
    with open(f'{SQL_DIR}/{file_name}.sql') as fil:
        line = ''
        while not line.startswith('-- Feature list:'):
            line = fil.readline()
        line = line.replace('-- Feature list:','').replace('\n','').replace(' ','')
#         feature_list = re.findall(' as (\S+),', raw_sql)
    return line.split(',')


def create_single_row_per_student_year(feature_list=None):
    """
        Groups by year and student, and aggregates all the raw features. During the aggregation, new features are created (avg, count...) according to the data type of the column.
        The returned SQL takes ~10min to run
    """
    conn, cur = create_pgconn()
    cur.execute('set role el_salvador_mined_education_write')
    if feature_list is None:
        feature_list = feature_list_from_SQL('features_raw')
        logger.debug(feature_list)
        
    aggreg_sql = """set role el_salvador_mined_education_write; drop table if exists results.features_aggregate;
        create table results.features_aggregate as (
            select 
                year_range, student,
                """
    
    aggregate_feature_list = ['year_range','student']
    constant_features = ['raw_age', 'student_female']
    for feature in feature_list:
        if feature not in ['year_range','student']:
            logger.debug(f'{feature}')
            cur.execute(f"select data_type from information_schema.columns where table_name = 'features_raw' and column_name='{feature}';")
            feature_type = cur.fetchall()[0][0]
            logger.debug(f'{feature}: {feature_type}')
            if feature in constant_features:
                # Constant features are the ones that if there are multiple values, it is due to data noise, and are hence set to null.
                aggreg_sql += f"case when array_length(array_agg(distinct {feature}), 1)=1 then (array_agg(distinct {feature}))[1] else null::{feature_type} end as {feature},"
                aggregate_feature_list += [f'{feature}']
            elif feature_type == 'boolean':
                aggreg_sql += f"bool_or({feature}) as {feature}_any, bool_and({feature}) as {feature}_all,"
                aggregate_feature_list += [f'{feature}_any']
                aggregate_feature_list += [f'{feature}_all']
            elif feature_type in ['bigint','smallint','double precision', 'numeric', 'integer']:
                aggreg_sql += f"avg({feature}) as {feature}_avg, max({feature}) as {feature}_max, min({feature}) as {feature}_min,"
                aggregate_feature_list += [f'{feature}_avg']
                aggregate_feature_list += [f'{feature}_max']
                aggregate_feature_list += [f'{feature}_min']
            elif feature_type == 'text':
                # Text features are aggregated by concatenating strings. e.g. if a student was in Ahuachapán and Usulután in the same year, the departamento column is 'Ahuachapán,Usulután'
                aggreg_sql += f"array_to_string(array_agg(distinct {feature}), ',')  as {feature}, array_length(array_agg(distinct {feature}), 1) as {feature}_count,"
                aggregate_feature_list += [f'{feature}', f'{feature}_count']
                
            else:
                logger.warn(f"Don't know how to convert {feature} with {feature_type}")
 
    aggreg_sql = f"-- Feature list: {','.join(aggregate_feature_list)}\n" + aggreg_sql[:-1] + " from results.features_raw group by year_range, student); create index aggregate_year_range_student_idx on results.features_aggregate(year_range, student);"
#     logger.error(aggreg_sql)
    with open(SQL_DIR + '/features_aggregate.sql', 'w+') as fil:
        fil.write(aggreg_sql)


def create_time_aggregates(feature_list=None):
    """
    Like in the previous aggregation, boolean features are simplified to any, and numerics are averaged.
    
    The time aggregation pulls from results.features_imputed, staging.labels, results.features_aggregate
    """
    
    conn, cur = create_pgconn()
    cur.execute('set role el_salvador_mined_education_write')
    if feature_list is None:
        imputed_features = set(feature_list_from_SQL('features_imputation'))
        nonimputed_features = set(feature_list_from_SQL('features_nonimputed'))
#         onehot_features = set(feature_list_from_SQL('features_onehot'))
        feature_list = imputed_features.union(nonimputed_features)#.union(onehot_features)
        logger.debug(feature_list)
    
    aggreg_sql = """set role el_salvador_mined_education_write; drop table if exists results.features_timeaggregates; create table results.features_timeaggregates as (select """
    
    aggregate_feature_list = ['year_range','student']
    for feature in feature_list:
        if feature not in ['year_range','student']:
            logger.debug(f'{feature}')
            if feature in imputed_features:
                cur.execute(f"select data_type from information_schema.columns where table_name = 'features_imputed' and column_name='{feature}';")
                dummy_table = 'a'  # dummy_table is just a short hand for calling columns, e.g. a.family_members
            elif feature in nonimputed_features:
                cur.execute(f"select data_type from information_schema.columns where table_name = 'features_aggregate' and column_name='{feature}';")
                dummy_table = 'c'
#             elif feature in onehot_features:
#                 cur.execute(f"select data_type from information_schema.columns where table_name = 'features_onehot' and column_name='{feature}';")
#                 dummy_table = 'd'
            else:
                logger.warned('Not caught by if/else')
            feature_type = cur.fetchall()[0][0]
            logger.debug(f'{feature}: {feature_type}')
            for window in ['1y','2y','3y']:
                if feature_type == 'boolean':
#                     for name in ['any', 'all']:
                    aggreg_sql += f"(bool_and({dummy_table}.{feature}) over w_{window})::int as {feature}_{window}_all,"
                    aggregate_feature_list += [f'{feature}_{window}_all']
                    aggreg_sql += f"(bool_or({dummy_table}.{feature}) over w_{window})::int as {feature}_{window}_any,"
                    aggregate_feature_list += [f'{feature}_{window}_any']
                elif feature_type in ['bigint','smallint','double precision', 'numeric', 'integer']:
                    for name in ['avg','max','min']:
                        aggreg_sql += f"{name}({dummy_table}.{feature}) over w_{window} as {feature}_{window}_{name},"
                        aggregate_feature_list += [f'{feature}_{window}_{name}']
                elif feature_type == 'text':
                    # Text features are aggregated by concatenating strings. e.g. if a student was in Ahuachapán and Usulután in the consecutive years, the departamento_1y column is 'Ahuachapán,Usulután' and the departamento_1y_count is 2
                    aggreg_sql += f"array_to_string(array_agg({dummy_table}.{feature}) over w_{window}, ',')  as {feature}_{window},"
                    aggreg_sql += f"array_length(string_to_array(array_to_string(array_agg(distinct {dummy_table}.{feature}) over w_{window}, ','), ','), 1) as {feature}_{window}_count,"
                    aggregate_feature_list += [f'{feature}_{window}', f'{feature}_{window}_count']
                else:
                    logger.warn(f"Don't know how to convert {feature} with {feature_type}")
    
    previous_dropout_sql = "sum(b.label) over w_label_1y as dropout_1y, sum(b.label) over w_label_2y as dropout_2y, sum(b.label) over w_label_3y as dropout_3y"
    aggregate_feature_list += ['dropout_1y','dropout_2y','dropout_3y']
    
    aggreg_sql = f"-- Feature list: {','.join(aggregate_feature_list)}\n" + aggreg_sql + previous_dropout_sql + " from results.features_imputed a left join staging.labels as b on a.student=b.student and a.year_range=b.year_range left join results.features_aggregate c on a.student=c.student and a.year_range=c.year_range window w_1y as (partition by a.student order by a.year_range asc rows between 1 preceding and current row), w_2y as (partition by a.student order by a.year_range asc rows between 2 preceding and current row), w_3y as (partition by a.student order by a.year_range asc rows between 3 preceding and current row), w_label_1y as (partition by a.student order by a.year_range asc rows between 1 preceding and 1 preceding), w_label_2y as (partition by a.student order by a.year_range asc rows between 2 preceding and 1 preceding), w_label_3y as (partition by a.student order by a.year_range asc rows between 3 preceding and 1 preceding)); create index timeaggregates_year_range_student_idx on results.features_timeaggregates(year_range, student);" # left join results.features_onehot d on a.student=d.student and a.year_range=d.year_range
    
    logger.debug(f'# Features: {len(aggregate_feature_list)}')
    with open(SQL_DIR + '/features_timeaggregates.sql', 'w+') as fil:
        fil.write(aggreg_sql)
    

def nonimputed_features():
    conn, cur = create_pgconn()
    cur.execute('set role el_salvador_mined_education_write')
    imputed_features = set(feature_list_from_SQL('features_imputation'))
    aggregate_features = set(feature_list_from_SQL('features_aggregate'))
    nonimputed_features = aggregate_features.difference(imputed_features)
    logger.debug(nonimputed_features)
    sql_statement = f"-- Feature list: {','.join(nonimputed_features)}\nset role el_salvador_mined_education_write; drop table if exists results.features_nonimputed; create table results.features_nonimputed as (select {','.join(nonimputed_features)} from results.features_aggregate); create index nonimputed_year_range_student_idx on results.features_nonimputed(year_range, student);"
    logger.debug(f'# Features: {len(nonimputed_features)}')
    with open(SQL_DIR + '/features_nonimputed.sql', 'w+') as fil:
        fil.write(sql_statement)

def one_hot_encode(features=('departamento','municipio','school_departamento','school_municipio','commute')):
    
    conn, cur = create_pgconn()
    cur.execute('set role el_salvador_mined_education_write')
    
    aggreg_sql = ''
    feature_list = []
    for feature in features:
        # The second thing to do with text features is to one_hot_encode them. e.g. have each of the departamentos as a column, so that the student that was in Ahuachapán and Usulután in the same year gets two 1s in those departamento columns and zeros everywhere else.
        cur.execute(f"select distinct {feature} from results.features_raw;")
        one_hot_columns = [x[0] for x in cur.fetchall()]
        logger.debug(one_hot_columns)
        if len(one_hot_columns) < 300:
            for col in one_hot_columns:
                if col is not None:
                    aggreg_sql += f"case when array['{col}'] <@ string_to_array({feature}, ',') then true else false end as {feature + '_' + col.replace(' ', '_').replace('.','')},"
                    feature_list += [f"{feature + '_' + col.replace(' ', '_')}"]
    sql_statement = f"-- Feature list: {','.join(feature_list)}\nset role el_salvador_mined_education_write; drop table if exists results.features_one_hot; create table results.features_one_hot as (select year_range, student, {aggreg_sql[:-1]} from results.features_imputed a); create index onehot_year_range_student_idx on results.features_one_hot(year_range, student);"
    logger.debug(f'# Features: {len(feature_list)}')
    with open(SQL_DIR + '/features_onehot.sql', 'w+') as fil:
        fil.write(sql_statement)
                
def type_cast():
    conn, cur = create_pgconn()
    cur.execute('set role el_salvador_mined_education_write')
    sql_stmnt = 'set role el_salvador_mined_education_write;'

    for sql_file, table in zip(['features_imputation','features_onehot','features_aggregate','features_timeaggregates_small'],['features_imputed','features_one_hot','features_aggregate','features_timeaggregates']):
        
        features = feature_list_from_SQL(f'{sql_file}')
        sql_stmnt += f"create table results.{table}_typecast as (select year_range, student"

        for feature in features:
            if feature not in ['year_range','student']:
    #             logger.debug(f'{feature}')
                cur.execute(f"select data_type from information_schema.columns where table_schema='results' and table_name = '{table}' and column_name='{feature}';")
                feature_type = cur.fetchall()[0][0]
                if feature_type in ['integer','boolean']:
                    logger.debug(f"{feature}: {feature_type}: int2")
                    sql_stmnt += f', {feature}::int::int2'
                elif feature_type in ['double precision', 'numeric']:
                    logger.debug(f"{feature}: {feature_type}: float8")
                    sql_stmnt += f', {feature}::float8'
                else:
                    logger.warn(f'{feature}: Type not caught')
        sql_stmnt += f" from results.{table});"
    with open(SQL_DIR + 'features_typecast.sql', 'w+') as fil:
        fil.write(sql_stmnt)
    
#     This code could be used to change how psycopg2 deals with types
#     def smallint(x, cur):
#         if x is None:
#             return None
#         else:
#             return np.uint8(x)
#     def smallfloat(x, cur):
#         if x is None:
#             return None
#         else:
#             return np.float16(x)

#     oid_dict = dict(int2=21, int8=20, int4=23, bool=16, numeric=1700, float4=700, float8=701)
#     for key, value in oid_dict.items():
#         if 'int' in key or key == bool:
#             new_type = psycopg2.extensions.new_type((value,), f"{key}2PY", smallint)
#         elif 'float' in key or key == 'numeric':
#             new_type = psycopg2.extensions.new_type((value,), f"{key}2PY", smallfloat)
#         psycopg2.extensions.register_type(new_type, conn)

if __name__ == '__main__':
#     logger.error('Checkpoint: 1')
#     create_single_row_per_student_year()
#     logger.error('Checkpoint: 2')
#     nonimputed_features()
#     logger.error('Checkpoint: 3')
#     one_hot_encode()
#     logger.error('Checkpoint: 4')
#     create_time_aggregates()
    type_cast()