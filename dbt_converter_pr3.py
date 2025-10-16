import os
import pandas as pd
import snowflake.connector
import re
from ruamel.yaml import YAML

# ---------- CONFIGURATION ----------
EXCEL_FILE = r"/Users/takvishal/Documents/dbt_conversion/dbt_converter/sf_table_inventory.xlsx"
DBT_PROJECT_DIR = r"/Users/takvishal/Documents/dbt_conversion/dbt_converter/models"
# -----------------------------------

# --- YAML setup ---
yaml_handler = YAML()
yaml_handler.preserve_quotes = True
yaml_handler.indent(mapping=2, sequence=4, offset=2)

# --- Snowflake connection ---
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
)
print("✅ Connected to Snowflake successfully.")

# --- Load Excel ---
df = pd.read_excel(EXCEL_FILE)
excel_map = {
    str(row["table_name"]).strip().lower(): (row["database"], row["schema"])
    for _, row in df.iterrows()
}
print(f"📘 Loaded Excel with {len(excel_map)} table mappings.")

# --- Regex to parse DDL ---
ddl_pattern = re.compile(
    r"""^\s*
    (?P<col_name>"?[\w\s]+"?)\s+
    [\w\(\),]+
    (?:\s+WITH\s+TAG\s+\((?P<tag_content>.+?)\))?
    (?:\s+COMMENT\s+'(?P<comment>.*?)')?
    ,?$""",
    re.IGNORECASE | re.VERBOSE | re.MULTILINE
)


def parse_ddl_to_dbt(ddl_string):
    columns = []
    for match in ddl_pattern.finditer(ddl_string):
        col_name = match.group("col_name").strip('"')
        tag_content = match.group("tag_content")
        desc_content = match.group("comment") or ""
        dbt_tags = []
        policy_tag = ""

        amber_tag = (
            "projects/tide-payment-prj-wip-iac-uk/locations/europe-west2/"
            "taxonomies/3457114866031680/policyTags/1513664955126269388"
        )
        red_tag = (
            "projects/tide-payment-prj-wip-iac-uk/locations/europe-west2/"
            "taxonomies/1160578347745627110/policyTags/4489624887166714321"
        )

        if tag_content:
            tag_pairs = re.findall(r"([\w\.]+)\s*=\s*'([^']+)'", tag_content)
            dbt_tags = [tag_value.lower() for _, tag_value in tag_pairs]
            if "amber" in dbt_tags:
                policy_tag = amber_tag
            elif "red" in dbt_tags:
                policy_tag = red_tag

        tag_final = {"policy_tags": policy_tag} if policy_tag else {}
        columns.append({
            "name": col_name,
            "description": desc_content,
            "meta": tag_final
        })
    return columns


def upsert_columns(existing_columns, new_columns, model_name):
    """Add missing columns or fill missing description/meta."""
    existing_by_name = {col["name"].lower(): col for col in existing_columns}
    logs = []

    for new_col in new_columns:
        name = new_col["name"]
        key = name.lower()

        if key in existing_by_name:
            existing_col = existing_by_name[key]
            changed = []

            if not existing_col.get("description") and new_col.get("description"):
                existing_col["description"] = new_col["description"]
                changed.append("description")

            if not existing_col.get("meta") and new_col.get("meta"):
                existing_col["meta"] = new_col["meta"]
                changed.append("meta")

            if changed:
                logs.append(f"🔁 [{model_name}] updated {', '.join(changed)} for {name}")

        else:
            existing_columns.append(new_col)
            logs.append(f"➕ [{model_name}] added new column: {name}")

    return logs


def find_all_schema_yml(root_dir):
    schema_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        if "target" in dirpath or ".dbt" in dirpath:
            continue
        for f in filenames:
            if f.lower() == "schema.yml":
                schema_files.append(os.path.join(dirpath, f))
    return schema_files


# --- Main process ---
schema_files = find_all_schema_yml(DBT_PROJECT_DIR)
print(f"🔍 Found {len(schema_files)} schema.yml files to process.")

for yaml_path in schema_files:
    print(f"\n📂 Processing: {yaml_path}")

    with open(yaml_path, "r") as f:
        yaml_data = yaml_handler.load(f) or {}

    models = yaml_data.get("models", [])
    file_logs = []

    for model in models:
        table = model.get("name")
        if not table:
            continue

        table_lc = table.lower()

        if table_lc not in excel_map:
            print(f"⚠️ Skipping {table} — not found in Excel map.")
            continue

        database, schema = excel_map[table_lc]

        # --- Skip if database/schema missing ---
        if not database or not schema or str(database).strip() in ("", "nan") or str(schema).strip() in ("", "nan"):
            print(f"⚠️ Skipping {table} — missing database or schema info in Excel.")
            continue

        # --- Fetch DDL ---
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT GET_DDL('TABLE', '{database}.{schema}.{table}')")
            ddl_string = cur.fetchone()[0]
        except Exception as e:
            print(f"❌ Failed to fetch DDL for {table}: {e}")
            continue
        finally:
            cur.close()

        # --- Parse & merge ---
        new_columns = parse_ddl_to_dbt(ddl_string)
        if "columns" not in model:
            model["columns"] = []

        logs = upsert_columns(model["columns"], new_columns, table)
        file_logs.extend(logs)

    # --- Write YAML (preserving formatting) ---
    with open(yaml_path, "w") as f:
        yaml_handler.dump(yaml_data, f)

    print(f"✅ Updated {yaml_path} ({len(file_logs)} changes)")
    for log in file_logs:
        print("   ", log)

print("\n🎉 All schema.yml files processed successfully.")