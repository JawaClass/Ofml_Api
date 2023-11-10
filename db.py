import re
import sqlite3
from sqlalchemy.exc import ProgrammingError, OperationalError
import pandas as pd
import sqlalchemy

# DB = sqlite3.connect('ofml.db', check_same_thread=False)
from sqlalchemy import text

from repository import Table

host = 'pdf2obs01.kn.local'  # 172.22.253.244
connection_string = f"mysql+mysqldb://root@{host}:3306/ofml"
engine = sqlalchemy.create_engine(connection_string, echo=False)

_db = None


def get_new_connection():
    return engine.connect()


def get_db_connection():
    global _db
    if _db is None:
        _db = engine.connect()

    if _db.closed:
        _db = engine.connect()

    return _db


def update_table(table: Table, program_name):
    sql_table_name = table.database_table_name

    sql_delete_table_entries = f"DELETE FROM {sql_table_name} WHERE __sql__program__='{program_name}';"

    try:
        execute_write(sql_delete_table_entries)
    except ProgrammingError:
        pass

    print(f'START: inserting {sql_table_name} of {program_name}')

    def _insert():

        try:
            table.df.to_sql(sql_table_name, get_db_connection(), if_exists='append', method='multi', chunksize=1000 * 1)
        except OperationalError as e:
            print('OperationalError START ...............')

            string = e._message()
            string = string.split()[2:]
            string = ' '.join(string).replace('"', '').replace('\'', '').replace(')', '')

            match = re.match("Unknown column (\w+?) in field list", string)

            if match:
                missing_column_name = match.groups()[0]
                missing_column_dtype = table.database_column_type(missing_column_name)

                sql_add_table = f"""
                        ALTER TABLE {sql_table_name}
                        ADD {missing_column_name} {missing_column_dtype}; 
                        """

                print(sql_add_table)
                execute_write(sql_add_table)
                input('...')
                _insert()

            else:
                raise NotImplementedError(f"todo... string={string}")

            input('OperationalError END ...............')
    _insert()
    print(f'END: inserting {sql_table_name} of {program_name}')


def execute_write(sql_cmd):
    with get_db_connection() as c:
        c.execute(text(sql_cmd))
        c.commit()


if __name__ == '__main__':
    print('Test')

    print(sqlalchemy.__version__)

    # import mysql-connector-python
    # host = 'pdf2obs01.kn.local'  # 172.22.253.244
    # connection_string = f"mysql+mysqldb://root@{host}:3306/ofml"
    # engi    ne = sqlalchemy.create_engine(connection_string, echo=True)
    with DB as connection:
        # pass
        _ = connection.execute(text('SHOW DATABASES;'))
        for r in _.mappings():
            print(r)
        print('_', _)
        _ = connection.commit()
        print('_', _)

        print('###')

        # connection.execute(text('SELECT * FROM [article];'))

# docker run -p 3306:3306 -d -v mysql_data:/var/lib/mysql mysql
