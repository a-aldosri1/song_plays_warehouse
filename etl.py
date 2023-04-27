import configparser
import psycopg2
from time import time
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    ''' Takes cusrsor and connection, load staging tables from s3 to redshift using copy_table_queries from sql_queries.py
    '''
    for query in copy_table_queries:


        print("======= LOADING TABLE:  =======")
        print(query)

        t0 = time()

        cur.execute(query)
        conn.commit()

        loadTime = time()-t0
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))


def insert_tables(cur, conn):
    ''' Takes cusrsor and connection, transform staging tables to fact and dimention tables using insert_table_queries from sql_queries.py
    '''
    for query in insert_table_queries:
        print("======= Inserting TABLE:  =======")
        print(query)

        t0 = time()


        cur.execute(query)
        conn.commit()

        loadTime = time()-t0
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    

    print('Begin copying data from s3 bucket to stagging tables')
    load_staging_tables(cur, conn)
    print('Done copying data from s3 bucket to stagging tables')

    print('Begin inserting tables')
    insert_tables(cur, conn)
    print('Done inserting tables')

    conn.close()


if __name__ == "__main__":
    main()