set role el_salvador_mined_education_write;

-- Feature list: san_salvador,la_libertad,santa_ana,sonsonate,san_miguel,usulutan,ahuachapan,la_paz,cuscatlan,la_union,san_vicente,morazan,chalatenango,cabañas,extranjero

drop table if exists results.features_dummy;
create table results.features_dummy as (
    select 
        year_range,student,age,
        overage1,overage_bool, overage1_1y_avg, overage1_2y_avg, overage1_3y_avg, 
        overage2, overage2_bool, overage2_1y_avg, overage2_2y_avg, overage2_3y_avg,
        repeats, repeats_bool, student_female, grado_code, family_members, 
        school_count_1y_avg, school_count_2y_avg, school_count_3y_avg, 
        school_mismatch, school_mismatch_1y_avg, school_mismatch_2y_avg, school_mismatch_3y_avg, 
        school_rural, school_public, 
        school_departamento_1y_count, school_departamento_2y_count, school_departamento_3y_count,
        case when array['san salvador'] <@ string_to_array(departamento, ',') then 1 else 0 end as san_salvador,
        case when array['la libertad'] <@ string_to_array(departamento, ',') then 1 else 0 end as la_libertad,
        case when array['santa_ana'] <@ string_to_array(departamento, ',') then 1 else 0 end as santa_ana,
        case when array['sonsonate'] <@ string_to_array(departamento, ',') then 1 else 0 end as sonsonate,
        case when array['san miguel'] <@ string_to_array(departamento, ',') then 1 else 0 end as san_miguel,
        case when array['usulutan'] <@ string_to_array(departamento, ',') then 1 else 0 end as usulutan,
        case when array['ahuachapan'] <@ string_to_array(departamento, ',') then 1 else 0 end as ahuachapan,
        case when array['la paz'] <@ string_to_array(departamento, ',') then 1 else 0 end as la_paz,
        case when array['cuscatlan'] <@ string_to_array(departamento, ',') then 1 else 0 end as cuscatlan,
        case when array['la union'] <@ string_to_array(departamento, ',') then 1 else 0 end as la_union,
        case  when array['san vicente'] <@ string_to_array(departamento, ',') then 1 else 0 end as san_vicente,
        case when array['morazan'] <@ string_to_array(departamento, ',') then 1 else 0 end as morazan,
        case when array['chalatenango'] <@ string_to_array(departamento, ',') then 1 else 0 end as chalatenango,
        case when array['cabañas'] <@ string_to_array(departamento, ',') then 1 else 0 end as cabañas,
        case when array['extranjero'] <@ string_to_array(departamento, ',') then 1 else 0 end as extranjero,
        sum(label) over w_1y as dropout_1y, 
        sum(label) over w_2y as dropout_2y, 
        sum(label) over w_3y as dropout_3y
    from 
        results.features_aggregate2
            left join staging.labels using (student, year_range)
    window 
        w_1y as (partition by student order by year_range asc rows between 1 preceding and current row), 
        w_2y as (partition by student order by year_range asc rows between 2 preceding and current row), 
        w_3y as (partition by student order by year_range asc rows between 3 preceding and current row)
)