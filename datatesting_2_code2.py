import pandas as pd
import os
import yaml

# ----------------------------
# Load YAML Configuration
# ----------------------------
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Input CSVs
TABLES_CSV = config['files']['tables_csv']
ROW_COUNTS_CSV = config['files']['row_counts_csv']
DESC_CSV = config['files']['desc_csv']

# Output directory
OUTPUT_DIR = config['files'].get('output_dir', 'output')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Output files
FILTERED_CSV = config['files'].get(
    'filtered_csv',
    os.path.join(OUTPUT_DIR, "filtered_columns.csv")
)
RECOMMENDATION_TXT = config['files'].get(
    'recommendation_txt',
    os.path.join(OUTPUT_DIR, "recommendation.txt")
)

# ----------------------------
# Load CSVs
# ----------------------------
try:
    tables_df = pd.read_csv(TABLES_CSV)
    row_counts_df = pd.read_csv(ROW_COUNTS_CSV)
    desc_df = pd.read_csv(DESC_CSV)
except Exception as e:
    print(f"Error loading CSVs: {e}")
    exit()

# Normalize columns for consistent access
tables_df.columns = [c.strip().lower().replace(" ", "_") for c in tables_df.columns]
row_counts_df.columns = [c.strip().lower().replace(" ", "_") for c in row_counts_df.columns]
desc_df.columns = [c.strip().lower().replace(" ", "_") for c in desc_df.columns]

# ----------------------------
# Ask user for column name to search
# ----------------------------
search_col = input("Enter the column name you want to search: ").strip().lower()

# ----------------------------
# Filter row_counts_df for the column
# ----------------------------
if 'column' not in row_counts_df.columns:
    print("Error: 'column' column not found in row_counts CSV.")
    exit()

filtered_df = row_counts_df[row_counts_df['column'].str.lower() == search_col]

if filtered_df.empty:
    with open(RECOMMENDATION_TXT, "w") as f:
        f.write(f"No column named '{search_col}' found.\n")
    print(f"No matches found for column '{search_col}'. Recommendation saved to {RECOMMENDATION_TXT}")
    exit()

# Save filtered CSV
filtered_df.to_csv(FILTERED_CSV, index=False)
print(f"Filtered column data saved to: {FILTERED_CSV}")

# ----------------------------
# Generate recommendation text
# ----------------------------
with open(RECOMMENDATION_TXT, "w") as f:
    f.write(f"Column Search Recommendation for '{search_col}'\n")
    f.write("=" * 60 + "\n\n")

    for idx, row in filtered_df.iterrows():
        table_name = row['table']
        row_count = row.get('row_count', 'N/A')
        total_count = row.get('total_count', 'N/A')
        distinct_count = row.get('distinct_count', 'N/A')
        null_count = row.get('null_count', 'N/A')

        # Get table + column descriptions
        desc_row = desc_df[
            (desc_df['table'].str.lower() == table_name.lower()) &
            (desc_df['column'].str.lower() == search_col)
        ]

        table_desc = desc_row['table_description'].values[0] if not desc_row.empty else "N/A"
        column_desc = desc_row['column_description'].values[0] if not desc_row.empty else "N/A"

        f.write(f"Table: {table_name}\n")
        f.write(f"Table Description: {table_desc}\n")
        f.write(f"Column: {search_col}\n")
        f.write(f"Column Description: {column_desc}\n")
        f.write(f"Row Count: {row_count}\n")
        f.write(f"Total Count: {total_count}\n")
        f.write(f"Distinct Count: {distinct_count}\n")
        f.write(f"Null Count: {null_count}\n")
        f.write("-" * 60 + "\n")

print(f"Recommendation text saved to: {RECOMMENDATION_TXT}")

# ----------------------------
# FIND ID COLUMNS + RELATIONS (uses row_counts_df)
# ----------------------------
ID_KEYWORDS = ["id", "_id"]

id_mapping = {}

# For every table where the search column exists
for table_name in filtered_df['table'].unique():

    # All columns of this table
    table_cols_df = row_counts_df[row_counts_df['table'].str.lower() == table_name.lower()]

    # Identify ID-like columns
    id_cols = [
        col.lower()
        for col in table_cols_df['column']
        if any(key in col.lower() for key in ID_KEYWORDS)
    ]

    # For each ID column find all tables that also contain it
    for id_col in id_cols:
        related_tables = row_counts_df[
            row_counts_df['column'].str.lower() == id_col
        ]['table'].unique().tolist()

        id_mapping[id_col] = related_tables

# ----------------------------
# Append ID relationships to recommendation file
# ----------------------------
with open(RECOMMENDATION_TXT, "a") as f:
    f.write("\nID Column Relationship Mapping\n")
    f.write("=" * 60 + "\n")

    if not id_mapping:
        f.write("No ID columns found.\n")
    else:
        for id_col, tables in id_mapping.items():
            f.write(f"\nID Column: {id_col}\n")
            f.write(f"Found In Tables: {', '.join(tables)}\n")
            f.write("-" * 60 + "\n")

print("ID column relationship mapping added to recommendation file.")
