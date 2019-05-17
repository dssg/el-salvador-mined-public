
# create indexes on cleaned data tables
SET role el_salvador_mined_education_write;

DROP INDEX if exists 6clean_id;
CREATE INDEX clean6_id ON cleaned."6_cleaned" (id);

DROP INDEX if exists 12clean_student;
CREATE INDEX clean12_student ON cleaned."12_cleaned" (student);


# analyze difference between 6 and 12

# number of students in 12_cleaned with more than one record
SELECT count(*) FROM (SELECT count(*) as cnt FROM cleaned."12_cleaned"
GROUP BY student
HAVING count(*) > 1);

# number of unique students present in 12_cleaned not in 6_cleaned
SELECT count(distinct(student)) 
FROM   cleaned."12_cleaned" c 
WHERE  NOT EXISTS (
   SELECT 
   FROM   cleaned."6_cleaned" d
   WHERE  c.student = d.id
   );

# count of unique students by grade in 12_cleaned
SELECT grado, count(distinct(student))
FROM cleaned."12_cleaned"
GROUP BY grado;

# count of unique students by grade in 6_cleaned
SELECT grado, count(distinct(id))
FROM cleaned."6_cleaned"
GROUP BY grado;

# distribution of 321 missing individuals by year 
SELECT event_date, count(*) FROM
(SELECT distinct(student), lower(year_range) as event_date, school 
FROM   cleaned."12_cleaned" c 
WHERE  NOT EXISTS (
   SELECT 
   FROM   cleaned."6_cleaned" d
   WHERE  c.student = d.id
   ))  as tmp
GROUP BY event_date;

# distribution of 321 missing individuals by grade
SELECT grado, count(*) from 
(SELECT distinct(student), grado
FROM   cleaned."12_cleaned" c 
WHERE  NOT EXISTS (
   SELECT 
   FROM   cleaned."6_cleaned" d
   WHERE  c.student = d.id
   ))  as tmp
GROUP BY grado;



### create semantic schema ###

DROP SCHEMA  if exists semantic cascade;
CREATE SCHEMA if not exists semantic;


# create student entities table in semantic schema

SET role el_salvador_mined_education_write;

DROP TABLE if exists semantic.entity_student;

CREATE TABLE semantic.entity_student AS
SELECT distinct(id) as student
FROM cleaned."6_cleaned";

CREATE INDEX student_entity_id on semantic.entity_student (student);

# create student events table in semantic schema

SET role el_salvador_mined_education_write;

DROP TABLE if exists semantic.events_student;

CREATE TABLE semantic.events_student as 
SELECT a.birth_date, a.gender, a.responsable, a.father, a.mother, a.address, a.bach_especialidad, a.bach_modalidad, a.bach_opcion, a.class_group, a.dpto, a.dpto_name, a.family_members, a.grado, a.grado_code, a.id as student, a.illness, a.medio_transporte as commute, a.munic, a.munic_name, a.school, a.status, lower(a.year_range) as event_date, b.promotion_status, now() as created_at
FROM 
cleaned."6_cleaned" as a
LEFT JOIN
cleaned."12_cleaned" as b
ON a.id = b.student and a.year_range = b.year_range and a.school = b.school and a.grado_code = b.grado_code; 

CREATE INDEX event_student_id on semantic.events_student (student);
CREATE INDEX events_student_school on semantic.events_student (school);


# crete school events table in semantic schema

#### Cleaning table 2 ####

CREATE TABLE cleaned."2_cleaned_stage" AS
  SELECT * FROM cleaned."2_cleaned";

UPDATE cleaned."2_cleaned_stage" SET parvularia = NULL WHERE parvularia = '"No especificado"' or parvularia = 'NR' or parvularia = 'SI' or parvularia = '"Si"';
UPDATE cleaned."2_cleaned_stage" SET parvularia = '0' WHERE parvularia = '"No"' or parvularia = 'NO';
ALTER TABLE cleaned."2_cleaned_stage" ALTER COLUMN parvularia TYPE INT USING parvularia::numeric::integer;

UPDATE cleaned."2_cleaned_stage" SET birth_certificate = NULL WHERE birth_certificate = 'No especificado' or birth_certificate = 'No disponible' or birth_certificate = 'No disponibles'
or birth_certificate = 'NO' or birth_certificate = 'No' or birth_certificate = 'Si' or birth_certificate = 'SI' or birth_certificate = 'S';
ALTER TABLE cleaned."2_cleaned_stage" ALTER COLUMN birth_certificate TYPE INT USING birth_certificate::numeric::integer;

ALTER TABLE cleaned."2_cleaned_stage" ADD COLUMN acc_ed_bool BOOLEAN;
UPDATE cleaned."2_cleaned_stage" SET acc_ed_bool = TRUE where accelerated_education = '1';
UPDATE cleaned."2_cleaned_stage" SET acc_ed_bool = FALSE where accelerated_education = '2';
UPDATE cleaned."2_cleaned_stage" SET acc_ed_bool = NULL::BOOL where accelerated_education = '0';

UPDATE cleaned."2_cleaned_stage" SET native_id = NULL WHERE native_id = 'No disponible' or native_id = '"No disponible"' or native_id= 'SI';
ALTER TABLE cleaned."2_cleaned_stage" ALTER COLUMN native_id TYPE INT USING native_id::numeric::integer;
UPDATE cleaned."2_cleaned_stage" SET nation_id = NULL WHERE nation_id = 'No disponible' or nation_id = '"No disponible"';
ALTER TABLE cleaned."2_cleaned_stage" ALTER COLUMN nation_id TYPE INT USING nation_id::numeric::integer;


SET role el_salvador_mined_education_write;


DROP TABLE if exists semantic.events_school;


CREATE TABLE semantic.events_school as 

with one as(
   select lower(year_range) as one_event_date, code as one_school, * from cleaned."1_cleaned"),

-- drop one.address

two as(
   select lower(year_range) as two_event_date, school_id as two_school, 
   count(school_id) as two_student_count,
   dsapp_mode(birth_certificate) FILTER (WHERE birth_certificate is not null) as birth_certificate, -- filter removes null obs, mode = most frequent non-null value
   avg(acc_ed_bool::int) as accelerated_education,
   -- acc_ed_bool is boolean whether student in accelerated ed
   dsapp_mode(disability_id) FILTER (WHERE disability_id is not null) as disability_id,
   dsapp_mode(economic_activity_id) FILTER (WHERE economic_activity_id is not null) as economic_activity_id,
   dsapp_mode(nation_id) FILTER (WHERE nation_id is not null) as nation_id,
   dsapp_mode(native_id) FILTER (WHERE native_id is not null) as native_id,
   avg(parvularia) as parvularia
   -- parvularia is number of years students in kindergarten
   from cleaned."2_cleaned_stage"
   group by two_event_date, two_school
),


-- use native id to generate variable if certain percentage students from different native region than school categorized in

four as (
   select lower(year_range) as four_event_date, school_id as four_school, 
   bool_and(ce_causas_desercion_presencia_pandillas) as ce_causas_desercion_presencia_pandillas, 
   bool_and(ce_causas_desercion_incorporacion_trabajo) as ce_causas_desercion_incorporacion_trabajo, 
   bool_and(factores_ce_drogas) as factores_ce_drogas, 
   bool_and(factores_ce_extorsiones) as factores_ce_extorsiones, 
   bool_and(factores_comunidad_explotacion_sexual) as factores_comunidad_explotacion_sexual, 
   bool_and(ce_causas_desercion_violencia_pandilleril) as ce_causas_desercion_violencia_pandilleril, 
   bool_and(factores_ce_violaciones) as factores_ce_violaciones, 
   bool_and(ce_causas_desercion_violencia_sexual) as ce_causas_desercion_violencia_sexual, 
   bool_and(factores_comunidad_maras) as factores_comunidad_maras, 
   bool_and(factores_comunidad_trata) as factores_comunidad_trata, 
   bool_and(ce_causas_desercion_embarazo) as ce_causas_desercion_embarazo, 
   bool_and(ce_causas_desercion_migracion) as ce_causas_desercion_migracion, 
   bool_and(factores_comunidad_violaciones) as factores_comunidad_violaciones, 
   bool_and(factores_comunidad_armas_blancas) as factores_comunidad_armas_blancas,
   bool_and(factores_ce_maras) as factores_ce_maras, 
   bool_and(ce_causas_desercion_acoso) as ce_causas_desercion_acoso, 
   bool_and(ce_causas_desercion_prostitucion) as ce_causas_desercion_prostitucion, 
   bool_and(factores_comunidad_robos) as factores_comunidad_robos
   from cleaned."4_cleanedriesgo" 
   group by four_event_date, four_school
),

six as (
   select lower(year_range) as six_event_date, school as six_school, count(school) as six_student_count
      from cleaned."6_cleaned"
      group by six_event_date, six_school
),

eight as (
      select school_id as eight_school, point(x_coord, y_coord) as coordinates1,
      ST_MakePoint(x_coord, y_coord) as coordinates2
      from cleaned."8_cleaned"
   )
-- only one year in eight- do we want to use anything besides cordinates
-- 5132 schools here 

select six_event_date as event_date, six_school as school, two_student_count - six_student_count as difference, * 
FROM six
   left join two on six_event_date = two_event_date and six_school = two_school
   left join four on six_event_date = four_event_date and six_school = four_school
   left join one on six_event_date = one_event_date and six_school = one_school
   left join eight on one_school = eight_school;


CREATE INDEX events_school_school on semantic.events_school (school);
CREATE INDEX events_school_event_date on semantic.events_school (event_date);

### Comments
# chose not to use table 3 (teacher census) bc missing data for 20% of schools (in 2014)


# create school entities table in semantic schema 
