# -*- coding: utf-8 -*-

from mined.utils import create_pgconn, logger, SQL_DIR

if __name__ == "__main__":
    conn, cur = create_pgconn()
    cur.execute('set role el_salvador_mined_education_write')
    
    file_names = ['create_semantic_schools','create_semantic_students','create_semantic_events']
    for file_name in file_names:
        logger.debug(f'Running: {file_name}')
        with open(f'{SQL_DIR}/{file_name}.sql') as fil:
            sql_file = fil.read()
        cur.execute(sql_file)
    conn.commit()
    logger.debug('Committed connection')