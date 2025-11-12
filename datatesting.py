import re
import csv

def extract_table_column_mapping(sql_file, output_csv="table_column_mapping.csv", default_schema="pub_glbl_medical"):
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql = f.read()

    sql = sql.replace('\n', ' ').strip()

    # Extract FROM and JOIN tables
    alias_map = {}
    schema_map = {}
    table_order = []
    table_pattern = re.compile(
        r'(?:from|join)\s+([\w\.]+)(?:\s+(?!on|where|order|group|having|limit)(\w+))?',
        re.IGNORECASE
    )

    for full_table, alias in table_pattern.findall(sql):
        if '.' in full_table:
            schema, table = full_table.split('.', 1)
        else:
            schema, table = default_schema, full_table

        alias = alias.strip() if alias else table
        alias_map[alias] = table
        schema_map[alias] = schema
        table_order.append((schema, table, alias))

    # Extract columns between SELECT and FROM
    select_match = re.search(r'select\s+(.*?)\s+from', sql, re.IGNORECASE)
    if not select_match:
        print("No SELECT found.")
        return

    columns = [c.strip() for c in re.split(r',\s*(?![^()]*\))', select_match.group(1))]

    # Prepare mapping
    mapping = []
    default_table_info = table_order[0] if table_order else (default_schema, "UNKNOWN", "UNKNOWN")

    for c in columns:
        if '.' in c:
            alias, col = c.split('.', 1)
            table = alias_map.get(alias.strip(), alias.strip())
            schema = schema_map.get(alias.strip(), default_schema)
            mapping.append((schema, table, alias.strip(), col.strip()))
        else:
            schema, table, alias = default_table_info
            mapping.append((schema, table, alias, c.strip()))

    # Write CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Schema Name", "Table Name", "Alias Name", "Column Name"])
        writer.writerows(mapping)

    print(f"Mapping document created: {output_csv}")

# Example
extract_table_column_mapping("input.sql")
