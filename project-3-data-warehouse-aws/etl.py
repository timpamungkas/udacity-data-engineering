import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load JSON data (for event and song data) from S3, then
    insert them into staging_events and staging_songs redshift tables.
    """
    for query in copy_table_queries:
        print("Start executing {}".format(query))
        cur.execute(query)
        conn.commit()
        print("Done executing {}".format(query))


def insert_tables(cur, conn):
    """
    Inserting data from staging tables (staginge events & songs) into actual table.
    So the data for actual tables is defined based on query to select from staging tables. 
    """
    for query in insert_table_queries:
        print("Start inserting {}".format(query))
        cur.execute(query)
        conn.commit()
        print("Done inserting {}".format(query))


def main():
    """
    Connect to AWS redshift cluster
    Load data from JSON on S3 into staging tables.
    Selecting data from staging table, into actual tables. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()