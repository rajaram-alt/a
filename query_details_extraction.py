import re
import pandas as pd

def analyze_sql_file(input_path, output_path="analysis_output.txt"):
    # === Read SQL file ===
    with open(input_path, 'r', encoding='utf-8') as f:
        sql = f.read()

    # Normalize whitespace
    sql = re.sub(r'\s+', ' ', sql.strip())

    # === Extract all CTE definitions ===
    ctes = {}
    cte_matches = re.findall(r'(\w+)\s+AS\s*\(', sql, re.IGNORECASE)
    cte_names = [cte.lower() for cte in cte_matches]  # store lowercase CTE names

    # Capture CTE SQL bodies
    for name in cte_names:
        m = re.search(rf'{name}\s+AS\s*\((.*?)\)\s*(?:,|SELECT|$)', sql, re.IGNORECASE | re.DOTALL)
        if m:
            ctes[name] = m.group(1).strip()
        else:
            ctes[name] = ""

    # === Extract Tables ===
    table_pattern = re.compile(r'(?:FROM|JOIN)\s+([\w\.]+)', re.IGNORECASE)
    all_tables = {t.lower() for t in table_pattern.findall(sql)}

    # Remove all CTEs from table list (case-insensitive)
    tables = sorted([t for t in all_tables if t not in cte_names])

    # Extract schema names
    schemas = sorted({t.split('.')[0] for t in tables if '.' in t})

    # === Build CTE Dependencies ===
    deps = []
    for cte_name, cte_sql in ctes.items():
        used_ctes = []
        for other_name in cte_names:
            if other_name != cte_name and re.search(r'\b' + re.escape(other_name) + r'\b', cte_sql, re.IGNORECASE):
                used_ctes.append(other_name)
        deps.append({
            "CTE_Name": cte_name,
            "Depends_On": ", ".join(used_ctes) if used_ctes else "None"
        })

    # === Helper for section formatting with underline ===
    def section(title, content):
        underline = "-" * len(title)
        return f"{title}\n{underline}\n{content if content else 'None'}\n\n"

    # === Build the report ===
    output = ""
    output += section("SCHEMAS USED:", "\n".join(schemas))
    output += section("TABLES USED:", "\n".join(tables))
    output += section("CTEs FOUND:", "\n".join(sorted(cte_names)))
    output += "CTE DEPENDENCIES:\n" + "-" * len("CTE DEPENDENCIES:") + "\n"
    if deps:
        for d in deps:
            output += f"{d['CTE_Name']} -> {d['Depends_On']}\n"
    else:
        output += "None\n"

    # === Save to text file ===
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output.strip())

    print(f"✅ Analysis complete. Results saved to: {output_path}")

# === Run Example ===
if __name__ == "__main__":
    analyze_sql_file("input_query.sql", "query_details.txt")
