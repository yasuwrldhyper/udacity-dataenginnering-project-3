import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load staging Table

    Load log_data, song_data to staging tables

    Args:
        cur (cursor): redshift cursor
        conn (connection): connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inset Tables

    process staging data into analytics tables on Redshift.

    Args:
        cur (cursor): redshift cursor
        conn (connection): connection
    """
    for query in insert_table_queries:
        # print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    db_config = "host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values())

    conn = psycopg2.connect(db_config)
    cur = conn.cursor()

    print("====== start load staging tables ====")
    load_staging_tables(cur, conn)

    print("====== start insert tables ====")
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
