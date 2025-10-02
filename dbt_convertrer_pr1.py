import os
import pandas as pd
import snowflake.connector
import re
from ruamel.yaml import YAML

# ---------- CONFIGURATION ----------
EXCEL_FILE = r"C:\Users\p.pravinkumaar\Documents\dbt tagging\dbt_converter-1\sf_table_inventory.xlsx"
DBT_PROJECT_DIR = r"C:\Users\p.pravinkumaar\Documents\dbt tagging\dbt_converter-1\models"
# -----------------------------------

yaml_handler = YAML()
yaml_handler.indent(mapping=2, sequence=4, offset=2)

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
    r"""^\s*
    (?P<col_name>\w+)\s+
    [\w\(\),]+
    (?:\s+WITH\s+TAG\s+\((?P<tag_content>.+?)\))?
    (?:\s+COMMENT\s+'(?P<comment>.*?)')?
    ,?$""",
    re.IGNORECASE | re.VERBOSE | re.MULTILINE
)

def parse_ddl_to_dbt(ddl_string):
    columns = []
    for match in ddl_pattern.finditer(ddl_string):
        col_name = match.group("col_name")
        tag_content = match.group("tag_content")
        desc_content = match.group("comment")
        dbt_tags = []

        amber_tag = "projects/tide-payment-prj-wip-iac-uk/locations/europe-west2/taxonomies/3457114866031680/policyTags/1513664955126269388"
        red_tag = "projects/tide-payment-prj-wip-iac-uk/locations/europe-west2/taxonomies/1160578347745627110/policyTags/4489624887166714321"
        policy_tag = ""

        if tag_content:
            tag_pairs = re.findall(r"([\w\.]+)\s*=\s*'([^']+)'", tag_content)
            dbt_tags = [tag_value.lower() for _, tag_value in tag_pairs]
            if 'amber' in dbt_tags:
                policy_tag = amber_tag
            elif 'red' in dbt_tags:
                policy_tag = red_tag

        tag_final = {"policy_tags": policy_tag} if dbt_tags else {}
        columns.append({
            "name": col_name,
            "description": desc_content,
            "meta": tag_final
        })
    return columns

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
            if model.get("name") == table:
                return path, data, model
    return None, None, None

def upsert_columns(existing_columns, new_columns, model_name):
    existing_by_name = {col["name"]: col for col in existing_columns}
    added = []
    updated = []

    for new_col in new_columns:
        name = new_col["name"]
        if name in existing_by_name:
            existing_col = existing_by_name[name]
            changes = []

            if not existing_col.get("description") and new_col.get("description"):
                existing_col["description"] = new_col["description"]
                changes.append("description")

            if not existing_col.get("meta") and new_col.get("meta"):
                existing_col["meta"] = new_col["meta"]
                changes.append("meta")

            if changes:
                updated.append((name, changes))

        else:
            existing_columns.append(new_col)
            added.append(name)

    # 🖨️ Logging
    if added:
        print(f"➕ [{model_name}] Columns added: {', '.join(added)}")
    if updated:
        for col_name, fields in updated:
            print(f"🔁 [{model_name}] Column '{col_name}' updated fields: {', '.join(fields)}")

# ------------------- Main Logic -------------------

schema_files = find_all_schema_yml(DBT_PROJECT_DIR)
yamls_to_write = {}

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

    new_columns = parse_ddl_to_dbt(ddl_string)
    yaml_path, yaml_data, existing_model = find_table_in_yamls(table, schema_files)

    if yaml_path:
        if "models" not in yaml_data:
            yaml_data["models"] = []
        model_found = False

        for model in yaml_data["models"]:
            if model.get("name") == table:
                if "columns" not in model:
                    model["columns"] = []
                upsert_columns(model["columns"], new_columns, table)
                model_found = True
                break

        if not model_found:
            print(f"➕ Adding full model '{table}' to existing schema.yml: {yaml_path}")
            yaml_data["models"].append({
                "name": table,
                "description": "",
                "columns": new_columns
            })

        yamls_to_write[yaml_path] = yaml_data
        print(table)

    else:
        # No existing YAML contains this model — create new or append to default
        model = {
            "name": table,
            "description": "",
            "columns": new_columns
        }

        folder = DBT_PROJECT_DIR
        default_yaml_path = os.path.join(folder, "schema.yml")

        if default_yaml_path in yamls_to_write:
            yamls_to_write[default_yaml_path]["models"].append(model)
        elif default_yaml_path in schema_files:
            with open(default_yaml_path) as f:
                existing_yaml = yaml_handler.load(f) or {}
            if "models" not in existing_yaml:
                existing_yaml["models"] = []
            existing_yaml["models"].append(model)
            yamls_to_write[default_yaml_path] = existing_yaml
        else:
            print(f"🆕 Creating new schema.yml for model '{table}'")
            new_yaml_data = {
                "version": 2,
                "models": [model]
            }
            yamls_to_write[default_yaml_path] = new_yaml_data

# ------------------- Write All YAMLs -------------------

for path, yaml_data in yamls_to_write.items():
    yaml_data["version"] = 2
    with open(path, "w") as f:
        yaml_handler.dump(yaml_data, f)
    print(f"✅ Written: {path}")

print("\n🎉 All tables processed. schema.yml files updated or created successfully.")
