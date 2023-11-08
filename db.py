import sqlite3
import sqlalchemy

# DB = sqlite3.connect('ofml.db', check_same_thread=False)
from sqlalchemy import text

host = 'pdf2obs01.kn.local'  # 172.22.253.244
connection_string = f"mysql+mysqldb://root@{host}:3306/ofml"
engine = sqlalchemy.create_engine(connection_string, echo=True)
DB = engine.connect()


def update_table():
    pass


def delete_table(table_name):
    #
    with DB as c:
        c.execute(f"DELETE FROM {table_name} WHERE __sql__program__='{self.name}';")
    pass


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
