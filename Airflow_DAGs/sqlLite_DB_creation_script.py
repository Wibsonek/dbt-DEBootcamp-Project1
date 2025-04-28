import sqlite3
import pandas as pd
import os
import logging


def create_tables_from_csv(sqlite_db_path, csv_dir_path):
    """
    Create tables from csv files in the SQLite DB.

    Input:  sqlite_db_path: string
            csv_dir_path: string

    Output: None
    """
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(sqlite_db_path)

    # Create a cursor object
    cursor = conn.cursor()

    # --------------------------------------------
    # Define table schemas

    # Customer table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS customers (
        customer_id TEXT PRIMARY KEY,
        customer_unique_id TEXT,
        customer_zip_code_prefix TEXT,
        customer_city TEXT,
        customer_state TEXT
    )
    ''')

    # Geolocation table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS geolocation (
        geolocation_zip_code_prefix TEXT PRIMARY KEY,
        geolocation_lat TEXT,
        geolocation_lng TEXT,
        geolocation_city TEXT,
        geolocation_state TEXT
    )
    ''')

    # Order Items table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS order_items (
        order_id TEXT PRIMARY KEY,
        order_item_id TEXT,
        product_id TEXT,
        seller_id TEXT,
        shipping_limit_date TEXT,
        price TEXT,
        freight_value TEXT
    )
    ''')

    # Order payments table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS payments (
        order_id TEXT PRIMARY KEY,
        payment_sequential TEXT,
        payment_type TEXT,
        payment_installments TEXT,
        payment_value TEXT
    )
    ''')

    # Order Reviews table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS order_reviews (
        review_id TEXT PRIMARY KEY,
        order_id TEXT,
        review_score TEXT,
        review_comment_title TEXT,
        review_comment_message TEXT,
        review_creation_date TEXT,
        review_answer_timestamp TEXT
    )
    ''')

    # Orders table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        order_status TEXT,
        order_purchase_timestamp TEXT,
        order_approved_at TEXT,
        order_delivered_carrier_date TEXT,
        order_delivered_customer_date TEXT,
        order_estimated_delivery_date TEXT
    )
    ''')

    # Products table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        product_id TEXT PRIMARY KEY,
        product_category_name TEXT,
        product_name_lenght TEXT,
        product_description_lenght TEXT,
        product_photos_qty TEXT,
        product_weight_g TEXT,
        product_length_cm TEXT,
        product_height_cm TEXT,
        product_width_cm TEXT
    )
    ''')

    # Sellers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS sellers (
        seller_id TEXT PRIMARY KEY,
        seller_zip_code_prefix TEXT,
        seller_city TEXT,
        seller_state TEXT
    )
    ''')

    # Product_category_name_translation table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS product_category_name_translation (
        product_category_name PRIMARY KEY,
        product_category_name_english TEXT
    )
    ''')

    # --------------------------
    # Fill the tables

    # dictionary maps the table name with the source csv file
    table_file_map = {"customers": "olist_customers_dataset.csv",
                      "geolocation": "olist_geolocation_dataset.csv",
                      "order_items": "olist_order_items_dataset.csv",
                      "payments": "olist_payments_dataset.csv",
                      "order_reviews": "olist_order_reviews_dataset.csv",
                      "orders": "olist_orders_dataset.csv",
                      "products": "olist_products_dataset.csv",
                      "sellers": "olist_sellers_dataset.csv",
                      "product_category_name_translation": "olist_product_category_name_translation.csv"}

    # map sqlite data types with pandas data types
    # where key is the pandas dtype and value the sqlite data type
    dtype_map = {"object": "TEXT", "int64": "INT", "float64": "REAL"}

    # dictionary maps the sqlite data type with python data type
    convert_to_dtype_map = {"TEXT": str, "INT": int, "REAL": float}

    def fill_table(table, source_filepath):
        """
        Populates the specified SQLite table with data from a source CSV file.

        This function reads data from a source CSV file corresponding to the given
        table, checks if the data types of the columns in the source file match the
        data types defined in the SQLite table schema, and converts the source data
        types if necessary to ensure compatibility.

        Input:  table (str): The name of the SQLite table to populate.
                source_filepath (str): filepath to the csv file corresponding to the table

        Returns:    None

        Notes:
            - The function assumes that the table schema and the source file share
              the same column names.
            - Source data types are mapped to SQLite types (`TEXT`, `INT`, `REAL`)
              using predefined mappings.
            - If a mismatch in data type is detected, the source column is cast to
              the appropriate type before insertion.
        """

        # Retrieve information about the table's columns
        cursor.execute(f"PRAGMA table_info({table});")
        columns_info = cursor.fetchall()

        # dictionary to store the column name : data type pair values
        column_type_dict = {col[1]: col[2] for col in columns_info}

        # load source file to data frame
        try:
            source_df = pd.read_csv(source_filepath)
        except FileNotFoundError:
            logging.warning(f"File not found: {source_filepath}")
            return
        except Exception as e:
            logging.error(f"Error reading {source_filepath}: {e}")
            return

        for column_name in column_type_dict.keys():

            # Skip missing columns
            if column_name not in source_df.columns:
                logging.info(f"Table: {table} Column {column_name} not present in the source file")
                continue

            source_column_dtype = str(source_df[column_name].dtype)
            table_column_dtype = column_type_dict[column_name]

            logging.info(f"Table: {table} "
                         f"Table column: {column_name}, "
                         f"Table dtype: {table_column_dtype}, "
                         f"Source dtype: {source_column_dtype}")

            if table_column_dtype != dtype_map[source_column_dtype]:
                source_df[column_name] = source_df[column_name].astype(convert_to_dtype_map[table_column_dtype])

            logging.info(f"Table: {table} "
                         f"Table column: {column_name}, "
                         f"Table dtype: {table_column_dtype}, "
                         f"Source dtype: {str(source_df[column_name].dtype)}")

        # Insert the DataFrame into the SQLite database
        source_df.to_sql(table, conn, if_exists='replace', index=False)

    for table, filename in table_file_map.items():
        source_path = os.path.join(csv_dir_path, filename)
        fill_table(table, source_path)

    conn.close()
