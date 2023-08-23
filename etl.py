# Import necessary libraries
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Define functions

def load_staging_tables(cur, conn):
    """
    Function to load data from S3 to the staging tables in Redshift Cluster.

    Args:
        cur: the cursor object.
        conn = connection to Redshift Cluster.
    """
    for query in copy_table_queries:
        print("\nCopying data: ", query)
        cur.execute(query)
        print("Data loading completed")
        conn.commit()


def insert_tables(cur, conn):
    """
    Function to insert data from staging tables to the analytics tables

    Args:
        cur: the cursor object.
        conn = connection to Redshift Cluster.
    """
    for query in insert_table_queries:
        print("\nInserting data: ", query)
        cur.execute(query)
        print("Data insertion completed")
        conn.commit()
        
        
def main():
    """
    Function to connect to the Redshift Cluster, then execute the load_staging_tables
    and insert_tables functions above.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

# main execution

if __name__ == "__main__":
    main()