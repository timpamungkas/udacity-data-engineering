import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop (if any) existing table from sparkify dwh
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error drop table: " + query)
            print(e)

def create_tables(cur, conn):
    """
    Create (if not exists) table for sparkify dwh
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error create table: " + query)
            print(e)


def main():
    """
    Connect to AWS Redshift, create new sparkifydb,
    drop existing tables, create new tables, close connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Connecting to redshift with host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print("Connected to redshift, now dropping and creating tables")
    drop_tables(cur, conn)
    create_tables(cur, conn)
    print("Dropping and creating tables done")

    conn.close()


if __name__ == "__main__":
    main()