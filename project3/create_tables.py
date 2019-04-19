import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("run query {}".format(query))
        cur.execute(query)
        conn.commit()
        print("done.")


def create_tables(cur, conn):
    for query in create_table_queries:
        print("run query {}".format(query))
        cur.execute(query)
        conn.commit()
        print("done.")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    try:
        drop_tables(cur, conn)
        create_tables(cur, conn)
    except Exception as e:
        print(e)

    conn.close()


if __name__ == "__main__":
    main()