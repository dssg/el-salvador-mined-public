SET ROLE el_salvador_mined_education_write;

DROP TABLE IF EXISTS raw.column_mapping;

CREATE TABLE IF NOT EXISTS raw.column_mapping (
    orginal_col_name VARCHAR,
    mapped_col_name VARCHAR,
    table_name VARCHAR
);
