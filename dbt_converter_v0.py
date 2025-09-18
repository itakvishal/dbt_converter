import snowflake.connector
import pandas as pd
import os
from collections import defaultdict
import yaml


EXCEL_FILE = "/Users/takvishal/Documents/dbt_conversion/sf_table_inventory.xlsx"  # your excel input
OUTPUT_DIR = "/Users/takvishal/Documents/dbt_conversion/dbt_yaml_output/"  # where schema.yml files will be saved
# ----------------------------------

# ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# connect to Snowflake
#conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

# ---------- CONFIGURATION ----------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
)

# read Excel
df = pd.read_excel(EXCEL_FILE)

# group tables by (database, schema)
grouped = defaultdict(list)
for _, row in df.iterrows():
    grouped[(row["database"], row["schema"])].append(row["table_name"])

# query template to get column tags
TAG_QUERY = """
select c.column_name, t.tag_name, t.tag_value
from {db}.information_schema.columns c
left join {db}.information_schema.tag_references_all_columns t
    on c.table_catalog = t.object_database
    and c.table_schema = t.object_schema
    and c.table_name = t.object_name
    and c.column_name = t.column_name
where c.table_schema = %s and c.table_name = %s
order by c.ordinal_position
"""

# loop through each schema group
for (database, schema), tables in grouped.items():
    tables_list = []

    for table in tables:
        cur = conn.cursor()
        cur.execute(TAG_QUERY.format(db=database), (schema, table))
        rows = cur.fetchall()
        cur.close()

        # collect tags per column
        columns = {}
        for col_name, tag_name, tag_value in rows:
            if col_name not in columns:
                columns[col_name] = {"tags": []}
            if tag_name and tag_value:
                columns[col_name]["tags"].append(f"{tag_name}: {tag_value}")

        # build dbt table structure
        tables_list.append({
            "name": table,
            "columns": [
                {"name": col, "description": "", "meta": {"tags": tags["tags"]}}
                for col, tags in columns.items()
            ]
        })

    # build final schema.yml content
    schema_dict = {
        "version": 2,
        "models": tables_list
    }

    # write to schema.yml (one per schema)
    out_path = os.path.join(OUTPUT_DIR, f"{schema}_schema.yml")
    with open(out_path, "w") as f:
        yaml.safe_dump(schema_dict, f, sort_keys=False)

print("✅ schema.yml files created successfully!")