import os
import pandas as pd
import psycopg2
from psycopg2 import sql
from config import POSTGRES_CONFIG
import json
import logging

# Ensure the 'logs' directory exists
if not os.path.exists('logs'):
    os.makedirs('logs')

# Configure logging to write to a file in the logs folder
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),  # Log file location
        logging.StreamHandler()               # Also log to console (optional)
    ]
)

# Paths to the files
ORDER_REGION_A_PATH = 'order_region_a.csv'
ORDER_REGION_B_PATH = 'order_region_b.csv'

# Function to load CSV data
def load_data(file_path, password):
    if password not in ['order_region_a', 'order_region_b']:
        logging.error("Incorrect password for accessing file: %s", file_path)
        raise ValueError("Incorrect password for accessing the file.")
    logging.info("Loading data from file: %s", file_path)
    return pd.read_csv(file_path)

# Extract data from CSV files
try:
    data_a = load_data(ORDER_REGION_A_PATH, 'order_region_a')
    data_b = load_data(ORDER_REGION_B_PATH, 'order_region_b')
except Exception as e:
    logging.exception("Failed to load data from CSV files: %s", e)
    raise

# Add region columns
data_a['region'] = 'A'
data_b['region'] = 'B'

# Concatenate the data
data = pd.concat([data_a, data_b], ignore_index=True)
logging.info("Concatenated data from both regions")

# Transformations
try:
    data['total_sales'] = data['QuantityOrdered'] * data['ItemPrice']
    data['PromotionDiscount'] = data['PromotionDiscount'].apply(lambda x: float(json.loads(x)['Amount']))
    data['net_sales'] = data['total_sales'] - data['PromotionDiscount']
    logging.info("Applied transformations to data")
    
    # Remove duplicates and filter by net_sales
    data.drop_duplicates(subset='OrderId', inplace=True)
    data = data[data['net_sales'] > 0]
    logging.info("Removed duplicates and filtered out records with non-positive net_sales")
except Exception as e:
    logging.exception("Data transformation failed: %s", e)
    raise

# Connect to PostgreSQL and create table
try:
    conn = psycopg2.connect(
        dbname=POSTGRES_CONFIG['dbname'],
        user=POSTGRES_CONFIG['user'],
        password=POSTGRES_CONFIG['password'],
        host=POSTGRES_CONFIG['host'],
        port=POSTGRES_CONFIG['port']
    )
    cursor = conn.cursor()
    logging.info("Connected to PostgreSQL database")
    
    # Create sales_data table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sales_data (
        OrderId TEXT PRIMARY KEY,
        OrderItemId TEXT,
        QuantityOrdered INTEGER,
        ItemPrice REAL,
        PromotionDiscount REAL,
        total_sales REAL,
        region TEXT,
        net_sales REAL
    )
    ''')
    conn.commit()
    logging.info("Created sales_data table if it didn't already exist")
except Exception as e:
    logging.exception("Failed to connect to PostgreSQL or create table: %s", e)
    raise

# Load data into PostgreSQL
try:
    for _, row in data.iterrows():
        insert_query = sql.SQL('''
        INSERT INTO sales_data (OrderId, OrderItemId, QuantityOrdered, ItemPrice, PromotionDiscount, total_sales, region, net_sales)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (OrderId) DO NOTHING
        ''')
        cursor.execute(insert_query, (
            row['OrderId'], row['OrderItemId'], row['QuantityOrdered'], row['ItemPrice'],
            row['PromotionDiscount'], row['total_sales'], row['region'], row['net_sales']
        ))
    conn.commit()
    logging.info("Inserted data into sales_data table")
except Exception as e:
    logging.exception("Failed to insert data into PostgreSQL: %s", e)
    raise

# Validation SQL Queries
def validate_data(cursor):
    validation_queries = {
        "total_record_count": "SELECT COUNT(*) FROM sales_data",
        "total_sales_by_region": "SELECT region, SUM(total_sales) FROM sales_data GROUP BY region",
        "average_sales_per_transaction": "SELECT AVG(net_sales) FROM sales_data",
        "duplicate_order_id_check": "SELECT OrderId, COUNT(OrderId) FROM sales_data GROUP BY OrderId HAVING COUNT(OrderId) > 1"
    }
    
    results = {}
    try:
        for key, query in validation_queries.items():
            cursor.execute(query)
            results[key] = cursor.fetchall()
            logging.info("Executed validation query: %s", key)
    except Exception as e:
        logging.exception("Failed to execute validation queries: %s", e)
        raise
    
    return results

# Run validations and display
try:
    validation_results = validate_data(cursor)
    for k, v in validation_results.items():
        logging.info("Validation - %s: %s", k, v)
except Exception as e:
    logging.error("Validation failed: %s", e)

# Close the database connection
finally:
    cursor.close()
    conn.close()
    logging.info("Closed PostgreSQL database connection")
