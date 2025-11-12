import re
import pandas as pd

def extract_table_column_mapping(sql, default_schema="pub_glbl_medical"):
    sql = re.sub(r'\s+', ' ', sql.strip())

    # Step 1: Extract CTEs
    cte_pattern = re.compile(r'WITH (.*?) SELECT', re.IGNORECASE | re.DOTALL)
    match = cte_pattern.search(sql)
    cte_block = match.group(1) if match else ""
    main_select = sql[match.end()-6:] if match else sql  # -6 keeps SELECT

    # Split individual CTEs
    ctes = {}
    for cte_def in re.findall(r'(\w+)\s+AS\s*\((.*?)\)(?:,|$)', cte_block, re.IGNORECASE | re.DOTALL):
        cte_name, cte_query = cte_def
        ctes[cte_name.lower()] = cte_query

    results = []

    def find_tables_and_columns(query, alias_prefix=None):
        tables = re.findall(r'(\w+\.\w+)\s+(\w+)', query)
        for full_table, alias in tables:
            cols = re.findall(rf'{alias}\.(\w+)', query)
            for c in cols:
                results.append({
                    'Schema Name': full_table.split('.')[0],
                    'Table Name': full_table.split('.')[1],
                    'Alias Name': alias,
                    'Column Name': c
                })

    # Step 2: Expand each CTE into base tables
    for cte, query in ctes.items():
        find_tables_and_columns(query, alias_prefix=cte)

    # Step 3: Parse main SELECT
    find_tables_and_columns(main_select)

    df = pd.DataFrame(results).drop_duplicates()
    df['Schema Name'] = df['Schema Name'].fillna(default_schema)
    return df
sql = open("input.sql").read()
df = extract_table_column_mapping(sql)
df.to_csv("query_mapping.csv", index=False)

