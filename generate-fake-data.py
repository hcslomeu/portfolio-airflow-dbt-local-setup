import duckdb
import numpy as np
from faker import Faker
import uuid
import random
import os
import time
import pandas as pd

# Initial settings
start_time = time.time()
fake = Faker('en_GB')
random.seed(42)
np.random.seed(42)

# Paths
SEEDS_PATH = '/Users/humbertolomeu/portfolio-projects/portfolio-airflow-dbt/portfolio-airflow-dbt-data-warehouse/portfolio_dw/seeds/'
os.makedirs(SEEDS_PATH, exist_ok=True)
DB_PATH = os.path.join(SEEDS_PATH, 'data.duckdb')

# Connect to DuckDB (creates the database if it doesn't exist)
con = duckdb.connect(DB_PATH)


def generate_ni_number():
    """Generate a UK National Insurance Number (NI) in format: XX 99 99 99 X"""
    # First two letters (some combinations are invalid, but for fake data we simplify)
    prefix_letters = 'ABCEGHJKLMNPRSTWXYZ'
    first_letter = random.choice(prefix_letters)
    second_letter = random.choice(prefix_letters)

    # Six digits
    digits = ''.join([str(random.randint(0, 9)) for _ in range(6)])

    # Final letter (A, B, C, or D)
    suffix = random.choice('ABCD')

    return f"{first_letter}{second_letter} {digits[:2]} {digits[2:4]} {digits[4:6]} {suffix}"


def create_tables():
    """Create tables in the DuckDB database."""
    con.execute("""
    CREATE TABLE IF NOT EXISTS customers (
        id UUID PRIMARY KEY,
        name VARCHAR,
        date_of_birth DATE,
        national_insurance_number VARCHAR(16) UNIQUE,
        postcode VARCHAR(10),
        city VARCHAR(100),
        county VARCHAR(50),
        country VARCHAR(50),
        gender CHAR(1),
        phone VARCHAR(20),
        email VARCHAR(100) UNIQUE,
        registration_date DATE
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id UUID PRIMARY KEY,
        national_insurance_number VARCHAR(16),
        order_value DECIMAL(10,2),
        shipping_cost DECIMAL(10,2),
        discount_amount DECIMAL(10,2),
        coupon VARCHAR(50),
        delivery_street VARCHAR(100),
        delivery_number VARCHAR(10),
        delivery_district VARCHAR(100),
        delivery_city VARCHAR(100),
        delivery_county VARCHAR(50),
        delivery_country VARCHAR(50),
        order_status VARCHAR(30),
        order_date DATE,
        FOREIGN KEY (national_insurance_number) REFERENCES customers(national_insurance_number)
    )
    """)


def get_existing_ni_numbers():
    """Return a set with all registered National Insurance Numbers."""
    result = con.execute("SELECT national_insurance_number FROM customers").fetchall()
    return {row[0] for row in result} if result else set()


def generate_customer_batch(batch_size):
    """Generate a batch of customer data using Faker and return a DataFrame."""
    print(f"  Generating {batch_size} customers...")

    # Generate data in smaller chunks to avoid memory overload
    chunk_size = 10000
    chunks = []

    for chunk_start in range(0, batch_size, chunk_size):
        chunk_end = min(chunk_start + chunk_size, batch_size)
        chunk_data = []

        for _ in range(chunk_start, chunk_end):
            ni_number = generate_ni_number()
            # Remove spaces for email generation
            ni_clean = ni_number.replace(' ', '')
            chunk_data.append({
                'id': str(uuid.uuid4()),
                'name': fake.name(),
                'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
                'national_insurance_number': ni_number,
                'postcode': fake.postcode(),
                'city': fake.city(),
                'county': fake.county(),
                'country': 'United Kingdom',
                'gender': random.choice(['M', 'F']),
                'phone': fake.phone_number(),
                'email': f"{ni_clean.lower()}@example.co.uk",
                'registration_date': fake.date_between(start_date='-2y', end_date='today').isoformat()
            })

        # Create a DataFrame with the current chunk
        df_chunk = pd.DataFrame(chunk_data)

        # Remove NI duplicates within this chunk
        df_chunk = df_chunk.drop_duplicates(subset=['national_insurance_number'])

        # Add to the list of chunks
        chunks.append(df_chunk)

        print(f"  Generated {chunk_end}/{batch_size} records...")

    # Concatenate all chunks into a single DataFrame
    if chunks:
        df = pd.concat(chunks, ignore_index=True)
        # Remove duplicates between chunks
        df = df.drop_duplicates(subset=['national_insurance_number'])
        return df.head(batch_size)  # Ensure the maximum requested size
    return pd.DataFrame()


def generate_order_batch(ni_numbers, batch_size):
    """Generate a batch of orders for the given NI numbers using Pandas for better performance."""
    try:
        print(f"  Generating {batch_size} orders...")

        # Generate data in smaller chunks
        chunk_size = 10000
        chunks = []

        for chunk_start in range(0, batch_size, chunk_size):
            chunk_end = min(chunk_start + chunk_size, batch_size)
            chunk_data = []

            for _ in range(chunk_start, chunk_end):
                try:
                    # Select a random NI number
                    ni_number = random.choice(ni_numbers)

                    # Generate order data
                    total_value = round(random.uniform(50, 2000), 2)
                    has_discount = random.random() < 0.2  # 20% chance of discount
                    discount_amount = round(total_value * random.uniform(0.05, 0.2), 2) if has_discount else 0.0

                    # Generate a unique coupon code based on UUID if there's a discount
                    coupon = f"COUPON{str(uuid.uuid4())[:8].upper()}" if has_discount else None

                    chunk_data.append({
                        'order_id': str(uuid.uuid4()),
                        'national_insurance_number': ni_number,
                        'order_value': total_value,
                        'shipping_cost': round(random.uniform(5, 100), 2),
                        'discount_amount': discount_amount,
                        'coupon': coupon,
                        'delivery_street': fake.street_name(),
                        'delivery_number': fake.building_number(),
                        'delivery_district': fake.city_suffix(),
                        'delivery_city': fake.city(),
                        'delivery_county': fake.county(),
                        'delivery_country': 'United Kingdom',
                        'order_status': random.choice(['pending', 'paid', 'shipped', 'delivered', 'cancelled']),
                        'order_date': fake.date_between(start_date='-2y', end_date='today').isoformat()
                    })
                except Exception as e:
                    print(f"  Error generating order: {str(e)}")
                    continue

            if not chunk_data:
                continue

            try:
                # Create a DataFrame with the current chunk
                df_chunk = pd.DataFrame(chunk_data)
                chunks.append(df_chunk)

                print(f"  Generated {chunk_end}/{batch_size} orders...")
            except Exception as e:
                print(f"  Error creating DataFrame for chunk: {str(e)}")
                continue

        # Concatenate all chunks into a single DataFrame
        if chunks:
            df = pd.concat(chunks, ignore_index=True)
            print(f"  Total of {len(df)} orders generated successfully.")
            return df
        return pd.DataFrame()

    except Exception as e:
        print(f"Critical error in generate_order_batch: {str(e)}")
        return pd.DataFrame()


def batch_insert(table, df):
    """Insert data in batch using Pandas DataFrame."""
    if df.empty:
        return

    # Convert to DuckDB format
    con.register('temp_df', df)

    try:
        # Insert data, ignoring duplicates
        con.execute(f"""
            INSERT OR IGNORE INTO {table}
            SELECT * FROM temp_df
        """)

        # Count how many records were inserted
        result = con.execute("SELECT COUNT(*) as inserted FROM temp_df").fetchone()[0]
        print(f"  {result} records inserted into table {table}")

    except Exception as e:
        print(f"Error inserting data into table {table}: {str(e)}")
        raise
    finally:
        # Remove the temporary DataFrame
        con.unregister('temp_df')


def export_to_csv():
    """Export tables to CSV files."""
    con.execute(f"EXPORT DATABASE '{SEEDS_PATH}' (FORMAT CSV)")


def main():
    print("Starting data generation with DuckDB...")

    try:
        # Create tables if they don't exist
        create_tables()

        # Ensure tables are empty
        con.execute("DELETE FROM orders")
        con.execute("DELETE FROM customers")

        # Generate customers (10,000 records)
        print("Generating customers...")
        total_customers = 10_000
        customer_batch_size = 5_000  # Larger batch for better performance

        for i in range(0, total_customers, customer_batch_size):
            current_size = min(customer_batch_size, total_customers - i)
            print(f"Processing customers {i+1}-{i+current_size}...")

            # Generate and insert the customer batch
            df_customers = generate_customer_batch(current_size)
            if not df_customers.empty:
                batch_insert('customers', df_customers)

        # Get NI numbers of registered customers
        ni_numbers = con.execute("SELECT national_insurance_number FROM customers").fetchdf()['national_insurance_number'].tolist()

        # Generate orders (50,000 records)
        print("\nGenerating orders...")
        total_orders = 50_000
        order_batch_size = 5_000  # Batch size for processing

        for i in range(0, total_orders, order_batch_size):
            print(f"Processing orders {i+1}-{min(i+order_batch_size, total_orders)}...")
            data = generate_order_batch(ni_numbers, min(order_batch_size, total_orders - i))
            batch_insert('orders', data)

        # Statistics
        print("\nStatistics:")
        total_cust = con.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        total_ord = con.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        avg_orders = total_ord / total_cust if total_cust > 0 else 0

        print(f"- Total customers generated: {total_cust:,}")
        print(f"- Total orders generated: {total_ord:,}")
        print(f"- Average orders per customer: {avg_orders:.2f}")

        # Export to CSV
        print("\nExporting to CSV...")
        export_to_csv()

    finally:
        # Close connection
        con.close()

        # Remove temporary DuckDB file
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)

        # Remove temporary files (load.sql and schema.sql from seeds folder)
        if os.path.exists(SEEDS_PATH + 'load.sql'):
            os.remove(SEEDS_PATH + 'load.sql')
        if os.path.exists(SEEDS_PATH + 'schema.sql'):
            os.remove(SEEDS_PATH + 'schema.sql')

    elapsed_time = time.time() - start_time
    print(f"\nTotal execution time: {elapsed_time:.2f} seconds")


if __name__ == "__main__":
    main()
