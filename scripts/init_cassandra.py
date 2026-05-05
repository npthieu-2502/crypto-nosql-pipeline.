import os
from cassandra.cluster import Cluster
from dotenv import load_dotenv

load_dotenv()

CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')
CASSANDRA_PORT = int(os.getenv('CASSANDRA_PORT', 9042))

def init_db():
    print(f"Connecting to Cassandra ({CASSANDRA_HOST}:{CASSANDRA_PORT})...")
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    
    print("Creating Keyspace 'crypto_ks'...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS crypto_ks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('crypto_ks')
    
    print("Creating Table 'ohlc_1m'...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS ohlc_1m (
            symbol text,
            window_start timestamp,
            window_end timestamp,
            open_price double,
            high_price double,
            low_price double,
            close_price double,
            PRIMARY KEY (symbol, window_start)
        ) WITH CLUSTERING ORDER BY (window_start DESC);
    """)
    
    print("Cassandra Initialization Completed Successfully!")
    cluster.shutdown()

if __name__ == "__main__":
    init_db()
