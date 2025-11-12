import re
import pandas as pd

def extract_pub_glbl_medical_columns(sql, target_schema="pub_glbl_medical"):
    # Normalize whitespace
    sql = re.sub(r'\s+', ' ', sql.strip())

    # Remove CTEs entirely
    cte_pattern = re.compile(r'WITH .*?SELECT', re.IGNORECASE | re.DOTALL)
    match = cte_pattern.search(sql)
    main_select = sql[match.end()-6:] if match else sql  # keep SELECT keyword

    results = []

    # Step 1: Extract tables with schema.pub_glbl_medical only
    table_aliases = re.findall(rf'({target_schema}\.\w+)\s+(\w+)', main_select)
    table_alias_map = {alias: full_table for full_table, alias in table_aliases}

    # Step 2: Extract columns referencing those tables
    columns = re.findall(r'(\w+)\.(\w+)', main_select)
    for alias, col in columns:
        if alias in table_alias_map:
            full_table = table_alias_map[alias]
            results.append({
                'Schema Name': full_table.split('.')[0],
                'Table Name': full_table.split('.')[1],
                'Alias Name': alias,
                'Column Name': col
            })

    df = pd.DataFrame(results).drop_duplicates()
    df['Schema Name'] = df['Schema Name'].fillna(target_schema)
    return df

# Usage
sql = open("input.sql").read()
df = extract_pub_glbl_medical_columns(sql)
df.to_csv("query_mapping.csv", index=False)
