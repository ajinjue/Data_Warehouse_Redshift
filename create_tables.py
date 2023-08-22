# Import necessary libraries
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Define functions

def drop_tables(cur, conn):
    """
    Function to drop existing tables in the cluster.
    
    Args:
        cur: the cursor object.
        conn = connection to Redshift Cluster.
    """
    for query in drop_table_queries:
        print("Dropping table: ", query)
        cur.execute(query)
        print("Table dropped")
        conn.commit()


def create_tables(cur, conn):
    """
    Function to create new tables in the cluster. 
    
    Args:
        cur: the cursor object.
        conn = connection to Redshift Cluster.
    """
    for query in create_table_queries:
        print("\nCreating table: ", query)
        cur.execute(query)
        print("Table completed")
        conn.commit()


def main():
    """
    Function to connect to the Redshift Cluster, then execute the drop_tables
    and create_tables functions above.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()

# main execution

if __name__ == "__main__":
    main()