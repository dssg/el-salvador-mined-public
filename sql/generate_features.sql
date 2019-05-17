# feature ideas:
# where mother is NULL
# where father is NULL
# where mother and father is NULL
# where illness is not NULL

'''
 address           | text                     |           |          | 
 bach_especialidad | text                     |           |          | 
 bach_modalidad    | text                     |           |          | 
 bach_opcion       | text                     |           |          | 
 class_group       | text                     |           |          | 
 dpto              | text                     |           |          | 
 dpto_name         | text                     |           |          | 
 family_members    | smallint                 |           |          | 
 grado             | text                     |           |          | 
 grado_code        | integer                  |           |          | 
 student           | text                     |           |          | 
 illness           | text                     |           |          | 
 commute           | text                     |           |          | 
 munic             | text                     |           |          | 
 munic_name        | text                     |           |          | 
 school            | text                     |           |          | 
 status            | boolean                  |           |          | 
 event_date        | date                     |           |          | 
 promotion_status  | text                     |           |          | 
 created_at        | timestamp with time zone 
 '''


 with tmp as (select event_date - birth_date as age, * from semantic.events_student) select age - avg(age) over (PARTITION by grado_code)
         as overage, age, birth_date, event_date from tmp) as overage