CASE
WHEN year ISNULL THEN daterange(to_date(NULLIF(right(original_table, 4),''), 'YYYY'), (to_date(NULLIF(right(original_table, 4),''), 'YYYY') + interval '1 year')::date, '[)')
ELSE daterange(to_date(NULLIF(year,''), 'YYYY'), (to_date(NULLIF(year,''), 'YYYY') + interval '1 year')::date, '[)')
END as year_range,