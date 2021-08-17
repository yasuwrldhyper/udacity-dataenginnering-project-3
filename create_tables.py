import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop Table

    Drop all tables

    Args:
        cur (cursor): redshift cursor
        conn (connection): connection
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create Table

    Create all tables

    Args:
        cur (cursor): redshift cursor
        conn (connection): connection
    """

    for query in create_table_queries:
        # print("create_table {}".format(query))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    cur = conn.cursor()

    print("====== start drop tables ====")
    drop_tables(cur, conn)

    print("====== start create tables ====")
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
