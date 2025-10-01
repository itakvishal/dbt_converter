import os
import pandas as pd
import snowflake.connector
import re
from collections import defaultdict
from ruamel.yaml import YAML

# ---------- CONFIGURATION ----------
EXCEL_FILE = r"C:\Users\p.pravinkumaar\Documents\dbt tagging\dbt_converter-1\sf_table_inventory.xlsx"
DBT_PROJECT_DIR = r"C:\Users\p.pravinkumaar\Documents\dbt tagging\dbt_converter-1"
""
# -----------------------------------

yaml_handler = YAML()
yaml_handler.indent(mapping=2, sequence=4, offset=2)
yaml_handler.preserve_quotes = True

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

# Regex to parse DDL columns and tags
ddl_pattern = re.compile(
    r"^\s*(?P<col_name>\w+)\s+[\w\(\),]+(?:\s+WITH\s+TAG\s+\((?P<tag_content>.+?)\))?,?$",
    re.IGNORECASE | re.MULTILINE
)

def parse_ddl_to_dbt(ddl_string):
    columns = []
    for match in ddl_pattern.finditer(ddl_string):
        col_name = match.group("col_name")
        tag_content = match.group("tag_content")
        dbt_tags = []
        amber_tag = "projects/tide-payment-prj-wip-iac-uk/locations/europe-west2/taxonomies/3457114866031680/policyTags/1513664955126269388"
        red_tag = "projects/tide-payment-prj-wip-iac-uk/locations/europe-west2/taxonomies/1160578347745627110/policyTags/4489624887166714321"
        if tag_content:
            tag_pairs = re.findall(r"([\w\.]+)\s*=\s*'([^']+)'", tag_content)
            dbt_tags = [tag_value.lower() for _, tag_value in tag_pairs]
            policy_tag ="1"
            if 'amber' in dbt_tags:
                policy_tag = amber_tag
           #     print(policy_tag)
            elif 'red' in dbt_tags:
                policy_tag = red_tag
           #     print(policy_tag)
      #  print(dbt_tags)

       # print(type(tag_content),type(policy_tag))
        tag_final = {"policy_tags": "- "+policy_tag} if dbt_tags else {}
        columns.append({
            "name": col_name,
            "description": "",
            "meta": tag_final
        })
    return columns

# ------------------- Recursive YAML Helpers -------------------

def find_all_schema_yml(root_dir):
    schema_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for f in filenames:
            if f.lower() == "schema.yml":
                schema_files.append(os.path.join(dirpath, f))
    return schema_files

def find_table_in_yamls(table_name, schema_files):
    for path in schema_files:
        with open(path) as f:
            data = yaml_handler.load(f) or {}
        models = data.get("models", [])
        for model in models:
            if model.get("name") == table_name:
                return path, data, model
    return None, None, None

# ------------------- Main Loop -------------------

schema_files = find_all_schema_yml(DBT_PROJECT_DIR)

for _, row in df.iterrows():
    database = row["database"]
    schema = row["schema"]
    table = row["table_name"]

    # Fetch DDL from Snowflake
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT GET_DDL('TABLE', '{database}.{schema}.{table}')")
        ddl_string = cur.fetchone()[0]
    finally:
        cur.close()

    # Convert DDL to dbt columns
    new_columns = parse_ddl_to_dbt(ddl_string)

    # Search table in existing YAML files
    yaml_path, yaml_data, existing_model = find_table_in_yamls(table, schema_files)

    if yaml_path:
        # Merge missing columns while preserving order
        existing_col_names = {c["name"] for c in existing_model.get("columns", [])}
        for col in new_columns:
            if col["name"] not in existing_col_names:
                existing_model.setdefault("columns", []).append(col)
        # Save updated YAML preserving formatting
        yaml_data["version"] = 2
        with open(yaml_path, "w") as f:
            yaml_handler.dump(yaml_data, f)
    else:
        # Table not found: create new model
        if schema_files:
            folder = os.path.dirname(schema_files[0])
        else:
            folder = DBT_PROJECT_DIR

        yaml_path = os.path.join(folder, "schema.yml")
        new_yaml_data = {
            "version": 2,
            "models": [
                {
                    "name": table,
                    "description": "",
                    "columns": new_columns
                }
            ]
        }
        with open(yaml_path, "w") as f:
            yaml_handler.dump(new_yaml_data, f)

print("✅ schema.yml files updated/created recursively in dbt project with formatting preserved!")
