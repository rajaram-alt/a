import psycopg2
import yaml
import os
import csv

# ----------------------------
# Load YAML Configuration
# ----------------------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

POSTGRES_HOST = config['postgres']['host']
POSTGRES_DB = config['postgres']['database']
POSTGRES_USER = config['postgres']['user']
POSTGRES_PASSWORD = config['postgres']['password']
SCHEMA_NAME = config['schema']

# ----------------------------
# Prepare output directory and paths
# ----------------------------
output_dir = os.path.join(os.getcwd(), "output")
os.makedirs(output_dir, exist_ok=True)

TABLES_LIST_CSV = os.path.join(output_dir, "tables_list.csv")
ROW_COUNT_QUERIES_SQL = os.path.join(output_dir, "row_count_queries.sql")
TABLE_ROW_COUNTS_CSV = os.path.join(output_dir, "table_row_counts.csv")

# ----------------------------
# Step 0: Ask for cardinality threshold
# ----------------------------
while True:
    try:
        cardinality_threshold = int(input("Enter cardinality threshold (distinct count < N will be tagged True): ").strip())
        break
    except ValueError:
        print("Please enter a valid integer.")

# ----------------------------
# Step 1: Generate Queries
# ----------------------------
user_input = input(f"Do you want to generate row count queries for tables in schema '{SCHEMA_NAME}'? (yes/no): ").strip().lower()
if user_input != 'yes':
    print("Operation cancelled.")
    exit()

try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    # Fetch tables
    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{SCHEMA_NAME}' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = cursor.fetchall()
    table_names = [t[0] for t in tables]

    if not table_names:
        print(f"No tables found in schema '{SCHEMA_NAME}'. Exiting.")
        exit()

    # ----------------------------
    # Save table list CSV
    # ----------------------------
    with open(TABLES_LIST_CSV, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Schema", "Table Name"])
        for table in table_names:
            writer.writerow([SCHEMA_NAME, table])
    print(f"Table list saved to: {TABLES_LIST_CSV}")

    # ----------------------------
    # Generate SQL file with table + column-level stats + Cardinality Tag
    # ----------------------------
    with open(ROW_COUNT_QUERIES_SQL, "w") as f:
        for table in table_names:
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{table}';
            """)
            columns = [col[0] for col in cursor.fetchall()]

            f.write(f"-- Table: {table}\n")
            union_queries = []
            for col in columns:
                q = f"""
SELECT 
    '{table}' AS Table_name,
    COUNT(*) AS row_count,
    '{col}' AS Column_name,
    COUNT("{col}") AS total_count,
    COUNT(DISTINCT "{col}") AS distinct_count,
    COUNT(*) - COUNT("{col}") AS null_count,
    CASE WHEN COUNT(DISTINCT "{col}") < {cardinality_threshold} THEN 'True' ELSE 'False' END AS cardinality_tag
FROM {SCHEMA_NAME}."{table}"
"""
                union_queries.append(q.strip())
            f.write("\nUNION ALL\n".join(union_queries) + ";\n\n")
    print(f"Row count and column stats queries saved to: {ROW_COUNT_QUERIES_SQL}")

except Exception as e:
    print(f"Error: {e}")
    exit()

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()

# ----------------------------
# Step 2: Execute Queries and Save CSV
# ----------------------------
user_input_execute = input(f"Do you want to execute the row count queries and save results? (yes/no): ").strip().lower()
if user_input_execute != 'yes':
    print("Execution cancelled.")
    exit()

try:
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    with open(TABLE_ROW_COUNTS_CSV, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Table", "row_count", "Column", "total_count", "Distinct Count", "Null Count", "Cardinality Tag"])

        for table in table_names:
            cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{table}';
            """)
            columns = [col[0] for col in cursor.fetchall()]

            for col in columns:
                cursor.execute(f"""
                    SELECT 
                        '{table}' AS Table_name,
                        COUNT(*) AS row_count,
                        '{col}' AS Column_name,
                        COUNT("{col}") AS total_count,
                        COUNT(DISTINCT "{col}") AS distinct_count,
                        COUNT(*) - COUNT("{col}") AS null_count,
                        CASE WHEN COUNT(DISTINCT "{col}") < {cardinality_threshold} THEN 'True' ELSE 'False' END AS cardinality_tag
                    FROM {SCHEMA_NAME}."{table}";
                """)
                writer.writerow(cursor.fetchone())

    print(f"\nRow counts and column details with cardinality tag saved to: {TABLE_ROW_COUNTS_CSV}")

except Exception as e:
    print(f"Error executing queries: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
