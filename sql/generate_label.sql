#### Label Def 1 #####
# Cohort = all students present on as_of_date
# Label = 1 if present on as_of_date and absent the following year
# Label = 0 if present on as_of_date and present the following year

cohort_config:
  # entities_table: 'testing_triage.active_facilities'
  # dense_states:
  #   table_name: 'states'
  #   state_filters:
  #     - 'active'
  query: |
    select distinct(student)
    from semantic.events_student
    where
    event_date = {as_of_date}::date
  name: 'student_cohort'

label_config:
  query: |

    with partitioned as (
    select student, 
    array_agg(distinct grado_code) as grado_codes, -- Aggregates of grado_codes and promotion_status within the year
    array_agg(distinct promotion_status) as passed,
    event_date,
    case 
        when 'GENERAL' = any(array_agg(bach_modalidad)) then 11  -- If the student is in any bachillerato general during the year, the final year is considered to be 11. This fails for students that are in General in 11, move to Vocacional in 11, and then dropout.
        when ARRAY['TECNICO VOCACIONAL', 'APREMAT', 'PILET'] && array_agg(bach_modalidad) then 12  -- If the student is in any of the other types of bachillerato, the final year is 12.
        else NULL::int end as final_bach_year,
    lead(event_date) over (partition by student order by event_date) as next_year 
    from semantic.events_student
    group by student, event_date)

    select student, event_date, grado_codes, passed, final_bach_year, next_year, case 
      #when next_year is not null
      when next_year = event_date + interval '1 year'  -- If a student is present the next year, they did not drop out
      then 0
      when final_bach_year <= any(grado_codes) and 'passed' = any(passed)  -- If they reached the final year of bachillerato and they passed, they did not drop out
      then 0
      else 1 end as label
      from partitioned limit 100;
    name: 'dropout'



'''
student | event_date | next_year  | label 
---------+------------+------------+-------
 1       | 2009-01-01 | 2011-01-01 |     0
 1       | 2011-01-01 | 2012-01-01 |     0
 1       | 2012-01-01 | 2014-01-01 |     0
 1       | 2014-01-01 |            |     1
 10      | 2012-01-01 |            |     1
 100     | 2009-01-01 |            |     0
 1000    | 2009-01-01 |            |     0
 10000   | 2014-01-01 |            |     1
 1000002 | 2009-01-01 | 2010-01-01 |     0
 1000002 | 2010-01-01 | 2011-01-01 |     0
 1000002 | 2011-01-01 | 2013-01-01 |     0
 1000002 | 2013-01-01 | 2014-01-01 |     0
 1000002 | 2014-01-01 |            |     0
 1000003 | 2009-01-01 | 2010-01-01 |     0
 1000003 | 2010-01-01 | 2011-01-01 |     0
 1000003 | 2011-01-01 | 2013-01-01 |     0
 1000003 | 2013-01-01 | 2014-01-01 |     0
 1000003 | 2014-01-01 |            |     0
 1000004 | 2009-01-01 | 2010-01-01 |     0
 1000004 | 2010-01-01 | 2011-01-01 |     0
 1000004 | 2011-01-01 | 2013-01-01 |     0
'''

#Current update
'''
student | event_date | next_year  | label 
---------+------------+------------+-------
 1       | 2009-01-01 | 2011-01-01 |     0
 1       | 2011-01-01 | 2012-01-01 |     0
 1       | 2012-01-01 | 2014-01-01 |     1
 1       | 2014-01-01 |            |     1
 10      | 2012-01-01 |            |     1
 100     | 2009-01-01 |            |     0
 1000    | 2009-01-01 |            |     0
 10000   | 2014-01-01 |            |     1
 1000002 | 2009-01-01 | 2010-01-01 |     0
 1000002 | 2010-01-01 | 2011-01-01 |     0
 1000002 | 2011-01-01 | 2013-01-01 |     1
 1000002 | 2013-01-01 | 2014-01-01 |     0
 1000002 | 2014-01-01 |            |     0
 1000003 | 2009-01-01 | 2010-01-01 |     0
 1000003 | 2010-01-01 | 2011-01-01 |     0
 1000003 | 2011-01-01 | 2013-01-01 |     1
 1000003 | 2013-01-01 | 2014-01-01 |     0
 1000003 | 2014-01-01 |            |     0
 1000004 | 2009-01-01 | 2010-01-01 |     0
 1000004 | 2010-01-01 | 2011-01-01 |     0
 1000004 | 2011-01-01 | 2013-01-01 |     1
 '''

### Baseline Test ###


SELECT event_date, COUNT(DISTINCT student) AS ct
  FROM semantic.events_student 
  GROUP BY event_date;

'''
   event_date |   ct    
------------+---------
 2008-01-01 |      46
 2009-01-01 | 1431179
 2010-01-01 | 1377143
 2011-01-01 | 1376228
 2012-01-01 | 1417210
 2013-01-01 | 1211591
 2014-01-01 | 1188037
 2015-01-01 | 1244642
 2016-01-01 | 1212675
 2017-01-01 | 1124258
 2018-01-01 |  675907
(11 rows)
'''


select format('select count(distinct(student)) from semantic.events_student where event_date = %L::date;', generate_series('2014-01-01', '2017-01-01', interval '1 year')); \gexec

'''
                                                     format                                                      
-----------------------------------------------------------------------------------------------------------------
 select count(distinct(student)) from semantic.events_student where event_date = '2014-01-01 00:00:00+00'::date;
 select count(distinct(student)) from semantic.events_student where event_date = '2015-01-01 00:00:00+00'::date;
 select count(distinct(student)) from semantic.events_student where event_date = '2016-01-01 00:00:00+00'::date;
 select count(distinct(student)) from semantic.events_student where event_date = '2017-01-01 00:00:00+00'::date;
(4 rows)

  count  
---------
 1188037
(1 row)

  count  
---------
 1244642
(1 row)

  count  
---------
 1212675
(1 row)

  count  
---------
 1124258
(1 row)
'''

create table semantic.label as

with partitioned as (
    select student, 
    array_agg(distinct grado_code) as grado_codes, -- Aggregates of grado_codes and promotion_status within the year
    array_agg(distinct promotion_status) as passed,
    event_date,
    case 
        when 'GENERAL' = any(array_agg(bach_modalidad)) then 11  -- If the student is in any bachillerato general during the year, the final year is considered to be 11. This fails for students that are in General in 11, move to Vocacional in 11, and then dropout.
        when ARRAY['TECNICO VOCACIONAL', 'APREMAT', 'PILET'] && array_agg(bach_modalidad) then 12  -- If the student is in any of the other types of bachillerato, the final year is 12.
        else NULL::int end as final_bach_year,
    lead(event_date) over (partition by student order by event_date) as next_year 
    from semantic.events_student
    group by student, event_date
    )
 select student, event_date, grado_codes, passed, final_bach_year, next_year, case 
      when next_year = event_date + interval '1 year'  -- If a student is present the next year, they did not drop out
      then 0
      when final_bach_year <= any(grado_codes) and 'passed' = any(passed)  -- If they reached the final year of bachillerato and they passed, they did not drop out
      then 0
      else 1 end as label
      from partitioned;

SELECT tmp.event_date, sum(tmp.label), count(tmp.label), sum(tmp.label)::decimal/count(tmp.label)::decimal as total_dropout FROM (SELECT DISTINCT student, event_date, label
  FROM semantic.label) as tmp 
  GROUP BY event_date;

 event_date |  sum   |  count  |     total_dropout      
------------+--------+---------+------------------------
 2017-01-01 | 465696 | 1124258 | 0.41422520453490213101
 2015-01-01 | 167355 | 1244642 | 0.13446035084787432852
 2008-01-01 |     17 |      46 | 0.36956521739130434783
 2013-01-01 | 271435 | 1211591 | 0.22403187214167157069
 2011-01-01 | 312129 | 1376228 | 0.22680035575500571126
 2012-01-01 | 423145 | 1417210 | 0.29857607552867958877
 2014-01-01 | 188049 | 1188037 | 0.15828547427394938036
 2018-01-01 | 658614 |  675907 | 0.97441511923977706992
 2010-01-01 | 303687 | 1377143 | 0.22051958293365322265
 2016-01-01 | 188077 | 1212675 | 0.15509266703774712928
 2009-01-01 | 333693 | 1431179 | 0.23315951393920676589

# write quwry to check # students in each student cohort and the label distribution for each year  (true dropout, false not dropout)
# for last 3 or 4 years
# query that gives history of one student - label status, event, score, real label, model predicted 
# think about which features to generate - triage generates aggregates against the as-of-date 
# dummy class, shallow DT, full DT, update model every year, predict 2017 going backwards


  