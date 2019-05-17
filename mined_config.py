# coding: utf-8

from sqlalchemy.event import listens_for
from sqlalchemy.pool import Pool

#this utility automatically sets database role, update the role below to role with write permissions for your database if applicable

@listens_for(Pool, "connect")
def assume_role(dbapi_con, connection_record):
    print("Triage is assuming the role!")
    dbapi_con.cursor().execute('set role <role>')
    print("Everything is OK!")
