''' This script drop tables and recreate them using drop_table_queries, create_table_queries defined in sql_queries.py
'''
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    ''' Takes cusrsor and connection, drop all tables in drop_table_queries from sql_queries.py
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    ''' Takes cusrsor and connection, create all tables defined in create_table_queries from sql_queries.py
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Begin droping and creating tables')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print('Tables created successfuly')

if __name__ == "__main__":
    main()