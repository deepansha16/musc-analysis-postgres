import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    # Connect to the default database
    conn = psycopg2.connect("dbname=udacity")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Create the sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # Close the connection to the default database
    conn.close()

    # Connect to the sparkify database
    conn = psycopg2.connect("dbname=sparkifydb")
    cur = conn.cursor()

    return cur, conn

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
