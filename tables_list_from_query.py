import re

def extract_tables(sql_file, output_file):
    # Read the SQL file
    with open(sql_file, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    # Normalize whitespace and remove comments
    sql = re.sub(r'--.*', '', sql)  # remove single line comments
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)  # remove block comments

    # Regex to find table references: schema.tablename or just tablename
    table_pattern = re.compile(r'\b(?:from|join|update|into)\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)', re.IGNORECASE)
    tables = set(table_pattern.findall(sql))

    # Save results to a txt file
    with open(output_file, 'w', encoding='utf-8') as f:
        for t in sorted(tables):
            f.write(t + '\n')

    print(f"Extracted {len(tables)} tables to {output_file}")

# Usage
extract_tables('input.sql', 'tables_list.txt')













































































































