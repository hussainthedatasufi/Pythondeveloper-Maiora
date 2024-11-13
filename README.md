# Pythondeveloper-Maiora
# Sales Data ETL

This project extracts, transforms, and loads sales data from two regions into a PostgreSQL database, followed by data validation queries.

## Setup and Running the Program

### Prerequisites
1. **Install required Python packages**:
    ```bash
    pip install pandas psycopg2
    ```

2. **Database Configuration**:
    Update the `config.py` file with your PostgreSQL connection settings. Ensure it includes:
    ```python
    POSTGRES_CONFIG = {
        'dbname': 'your_dbname',
        'user': 'your_user',
        'password': 'your_password',
        'host': 'your_host',
        'port': 'your_port'
    }
    ```

3. **Data Files**:
   Place the sales data files (`order_region_a.csv`, `order_region_b.csv`) in the project directory.

### Running the ETL Script
Run the ETL script using:
   ```bash
   python main.py


### Logging
Logs will be saved in the `logs` folder, capturing important actions and any errors that occur.

## Database

The ETL process will create a `sales_data` table in the specified PostgreSQL database. This table contains the transformed and cleaned data from both regions.

### Database Schema
- **OrderId**: TEXT, Primary Key
- **OrderItemId**: TEXT
- **QuantityOrdered**: INTEGER
- **ItemPrice**: REAL
- **PromotionDiscount**: REAL
- **total_sales**: REAL
- **region**: TEXT
- **net_sales**: REAL

## Assumptions

- **Passwords**: Hardcoded passwords simulate access control for loading the files.
- **Data Cleaning**: Records with `net_sales <= 0` are excluded.
- **Duplicates**: Duplicate `OrderId` entries are removed.

## Validation

The script includes SQL queries for validation:

- **Total Record Count**: Checks the total number of records in `sales_data`.
- **Total Sales by Region**: Aggregates `total_sales` by region.
- **Average Sales per Transaction**: Calculates the average `net_sales` per transaction.
- **Duplicate OrderId Check**: Ensures there are no duplicate `OrderId` entries.

