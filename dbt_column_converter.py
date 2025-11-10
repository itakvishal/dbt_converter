import os
import re
import pandas as pd
from ruamel.yaml import YAML

# ---------- CONFIG ----------
EXCEL_FILE = r"/Users/takvishal/Documents/dbt_conversion/dbt_converter/bq_partition_cluster.xlsx"
DBT_PROJECT_DIR = r"/Users/takvishal/Documents/dbt_conversion/dbt_converter/models"
# -----------------------------------

# ---------- Excel helpers ----------
def _csv_list(val):
    if val is None:
        return []
    return [c.strip() for c in str(val).split(",") if str(c).strip()]

def load_excel_rows(path: str):
    df = pd.read_excel(path)
    print(f"📘 Loaded Excel with {len(df)} records.")
    rows = []
    for _, row in df.iterrows():
        rows.append({
            "database_name": str(row.get("database_name", "")).strip(),
            "schema_name":   str(row.get("schema_name", "")).strip(),
            "table_name":    str(row.get("table_name", "")).strip(),  # not used for matching
            "cluster":       _csv_list(row.get("clustered_by_column", "")),
            "partition":     _csv_list(row.get("partition_by_column", "")),
        })
    return rows

# ---------- YAML helpers ----------
yaml = YAML()
yaml.preserve_quotes = True
yaml.indent(mapping=2, sequence=4, offset=2)

def list_schema_ymls(root_dir: str):
    paths = []
    for dirpath, _, filenames in os.walk(root_dir):
        if "target" in dirpath or ".dbt" in dirpath:
            continue
        for f in filenames:
            if f.lower() in ("schema.yml", "schema.yaml"):
                paths.append(os.path.join(dirpath, f))
    # stable order for deterministic runs
    paths.sort(key=lambda p: p.lower())
    return paths

# ---------- Find <model>.sql from PROJECT ROOT (filename match only) ----------
def find_sql_by_filename(project_root: str, model_name_lc: str):
    target = f"{model_name_lc}.sql"
    for dirpath, _, filenames in os.walk(project_root):
        if "target" in dirpath or ".dbt" in dirpath:
            continue
        for f in filenames:
            if f.lower() == target:
                return os.path.join(dirpath, f)
    return None

# ---------- SQL scanning + config injection ----------
CONFIG_RE = re.compile(r"(\{\{\s*config\s*\((.*?)\)\s*\}\})", re.DOTALL | re.IGNORECASE)

def infer_partition_type(col: str) -> str:
    c = col.lower()
    if "date" in c and not any(s in c for s in ["time", "ts", "stamp"]):
        return "DATE"
    return "TIMESTAMP"

def has_setting(inner: str, key: str) -> bool:
    return re.search(rf"\b{re.escape(key)}\s*=", inner) is not None

def is_view_materialization(inner_config: str) -> bool:
    return re.search(r"materialized\s*=\s*['\"]view['\"]", inner_config, flags=re.IGNORECASE) is not None

def format_cluster_list(cols):
    return "[" + ", ".join(f"'{c}'" for c in cols) + "]"

def update_existing_config(sql_text: str, partition_col, cluster_cols):
    """
    Update ONLY inside an existing {{ config(...) }} block.
    Returns: (updated_sql, status) where status in {'updated','no-op','no-config','is-view'}
    """
    m = CONFIG_RE.search(sql_text)
    if not m:
        return sql_text, "no-config"
    inner = m.group(2)

    if is_view_materialization(inner):
        return sql_text, "is-view"

    to_add = []
    if partition_col and not has_setting(inner, "partition_by"):
        dtype = infer_partition_type(partition_col)
        to_add.append(f"partition_by = {{'field': '{partition_col}', 'data_type': '{dtype}'}}")
    if cluster_cols and not has_setting(inner, "clustered_by"):
        to_add.append(f"clustered_by = {format_cluster_list(cluster_cols)}")

    if not to_add:
        return sql_text, "no-op"

    inner_clean = inner.strip()
    if inner_clean and not inner_clean.rstrip().endswith(","):
        inner_clean = inner_clean.rstrip() + ","

    replacement = "{{ config(\n"
    if inner_clean.strip():
        replacement += "    " + inner_clean.strip() + "\n    " + ",\n    ".join(to_add) + "\n) }}"
    else:
        replacement += "    " + ",\n    ".join(to_add) + "\n) }}"

    updated = sql_text[:m.start(1)] + replacement + sql_text[m.end(1):]
    return updated, "updated"

# ---------- Column presence in SQL ----------
def col_in_sql(sql_text: str, col: str) -> bool:
    """
    Check if a column name appears in SQL in common identifier forms:
    - bare:   ... col ...
    - dotted: a.col / t.col
    - quoted: `col` or "col"
    Case-insensitive, word-boundary-ish (avoid matching substrings).
    """
    c = re.escape(col)
    patterns = [
        rf"(?<!\w){c}(?!\w)",           # bare col
        rf"\.\s*{c}(?!\w)",             # .col
        rf"[`\"]\s*{c}\s*[`\"]",        # `col` or "col"
    ]
    for pat in patterns:
        if re.search(pat, sql_text, flags=re.IGNORECASE):
            return True
    return False

def excel_row_matches_sql(row, sql_text: str) -> bool:
    required = [c for c in (row["partition"] + row["cluster"]) if c]
    if not required:
        return False
    return all(col_in_sql(sql_text, c) for c in required)

# ---------- Choose best Excel row for a model by scanning the SQL ----------
def choose_row_for_model_sql(excel_rows, sql_text: str):
    candidates = []
    for r in excel_rows:
        if excel_row_matches_sql(r, sql_text):
            # score = number of specified columns (more specific is better)
            score = len([c for c in (r["partition"] + r["cluster"]) if c])
            candidates.append((score, r))
    if not candidates:
        return None, "no-match"
    candidates.sort(key=lambda x: (-x[0],))
    top_score = candidates[0][0]
    best = [r for s, r in candidates if s == top_score]
    if len(best) > 1:
        return None, "ambiguous"
    return best[0], "ok"

# ---------- Main: file-by-file over schema.yml, then models ----------
def main():
    excel_rows = load_excel_rows(EXCEL_FILE)
    if not excel_rows:
        print("⚠️ No usable rows in Excel. Exiting.")
        return

    schema_files = list_schema_ymls(DBT_PROJECT_DIR)
    print(f"🔍 Found {len(schema_files)} schema.yml files. Processing sequentially...")

    total_targets = 0
    total_updates = 0

    for yml_path in schema_files:
        print(f"\n📂 Schema file: {yml_path}")
        try:
            with open(yml_path, "r") as f:
                data = yaml.load(f) or {}
        except Exception as e:
            print(f"   ❌ Failed to parse YAML: {e}")
            continue

        models = data.get("models", [])
        if not isinstance(models, list) or not models:
            print("   ℹ️ No models key or empty — skipping.")
            continue

        for m in models:
            model_name = str(m.get("name", "")).strip()
            if not model_name:
                continue
            key = model_name.lower()

            # Locate <model>.sql strictly by filename from ROOT
            sql_path = find_sql_by_filename(DBT_PROJECT_DIR, key)
            if not sql_path or not os.path.exists(sql_path):
                print(f"   ⚠️ [{model_name}] could not find {key}.sql from root — skipping.")
                continue

            with open(sql_path, "r") as fh:
                sql_text = fh.read()

            # Pick best Excel row by checking column presence inside the SQL text
            chosen, status = choose_row_for_model_sql(excel_rows, sql_text)
            if status == "no-match":
                # No Excel row’s columns all appear in this SQL → nothing to do
                continue
            if status == "ambiguous":
                print(f"   ⚠️ [{model_name}] multiple Excel rows match by columns — skipping to avoid wrong edit.")
                continue

            target_partition = chosen["partition"][0] if chosen["partition"] else None  # single field
            target_clusters  = chosen["cluster"]

            if not target_partition and not target_clusters:
                continue

            total_targets += 1
            updated_sql, ustatus = update_existing_config(sql_text, target_partition, target_clusters)

            if ustatus == "no-config":
                print(f"   ⏭️ [{model_name}] has no {{ config(...) }} block — not creating one.")
                continue
            if ustatus == "is-view":
                print(f"   ⏭️ [{model_name}] materialized='view' — cannot partition/cluster views.")
                continue
            if ustatus == "no-op":
                print(f"   ℹ️ [{model_name}] already configured — no change.")
                continue

            with open(sql_path, "w") as fh:
                fh.write(updated_sql)
            total_updates += 1

            bits = []
            if target_partition: bits.append(f"partition_by.field={target_partition}")
            if target_clusters:  bits.append(f"clustered_by={target_clusters}")
            print(f"   ✅ [{model_name}] Updated {sql_path}: " + "; ".join(bits))

    print(f"\n🎉 Completed. {total_updates}/{max(total_targets,1)} SQL model(s) updated.")

if __name__ == "__main__":
    main()