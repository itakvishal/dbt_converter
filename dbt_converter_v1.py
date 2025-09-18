import os
import pandas as pd
import snowflake.connector
import re
import yaml
from collections import defaultdict

# ---------- CONFIGURATION ----------
EXCEL_FILE = "/Users/takvishal/Documents/dbt_conversion/dbt_converter/sf_table_inventory.xlsx"
OUTPUT_DIR = "/Users/takvishal/Documents/dbt_conversion/dbt_converter/dbt_yaml_output/"
# -----------------------------------

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
)

# Read Excel inventory
df = pd.read_excel(EXCEL_FILE)

# Group tables by (database, schema)
grouped = defaultdict(list)
for _, row in df.iterrows():
    grouped[(row["database"], row["schema"])].append(row["table_name"])

# Regex to parse DDL column lines
ddl_pattern = re.compile(
    r"^\s*(?P<col_name>\w+)\s+[\w\(\),]+(?:\s+WITH\s+TAG\s+\((?P<tag_content>.+?)\))?,?$",
    re.IGNORECASE | re.MULTILINE
)

# Function to parse DDL string into columns + tags
def parse_ddl(ddl_string):
    columns = []
    for match in ddl_pattern.finditer(ddl_string):
        col_name = match.group("col_name")
        tag_content = match.group("tag_content")
        tags = []
        if tag_content:
            tag_pairs = re.findall(r"([\w\.]+)\s*=\s*'([^']+)'", tag_content)
            tags = [f"{tag_name}: {tag_value}" for tag_name, tag_value in tag_pairs]

        columns.append({
            "name": col_name,
            "description": "",
            "meta": {"tags": tags} if tags else {}
        })
    return columns

# Process each schema group
for (database, schema), tables in grouped.items():
    models = []

    for table in tables:
        cur = conn.cursor()
        try:
            # ✅ Use GET_DDL to fetch table definition
            cur.execute(f"SELECT GET_DDL('TABLE', '{database}.{schema}.{table}')")
            ddl_string = cur.fetchone()[0]  # Full CREATE TABLE DDL
        finally:
            cur.close()

        # Parse the DDL to extract columns and tags
        columns = parse_ddl(ddl_string)

        models.append({
            "name": table,
            "description": "",
            "columns": columns
        })

    # Build schema.yml per schema
    schema_yml = {
        "version": 2,
        "models": models
    }

    out_file = os.path.join(OUTPUT_DIR, f"{schema}_schema.yml")
    with open(out_file, "w") as f:
        yaml.safe_dump(schema_yml, f, sort_keys=False)

print("✅ schema.yml files created successfully!")
