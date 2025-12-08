# python
import json
import traceback
from pathlib import Path

import pandas as pd
import yaml

from utils import write_dataframe_to_db


def _sanitize_table_name(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name).lower()


def _yaml_to_dataframe(yaml_obj):
    if isinstance(yaml_obj, dict):
        row = {
            k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
            for k, v in yaml_obj.items()
        }
        return pd.DataFrame([row])
    elif isinstance(yaml_obj, list):
        processed = []
        for item in yaml_obj:
            if isinstance(item, dict):
                processed.append(
                    {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in item.items()}
                )
            else:
                processed.append({"value": json.dumps(item)})
        return pd.DataFrame(processed)
    else:
        return pd.DataFrame([{"value": yaml_obj}])


def write_datasets_to_db(datasets_path: str = "../tabarena_datasets", list_path: str = "../tabarena_list.txt"):
    base = Path(datasets_path)
    list_file = Path(list_path)

    if not base.exists():
        print(f"ERROR: Datasets path does not exist: {base}")
        return

    if not list_file.exists():
        print(f"ERROR: List file does not exist: {list_file}")
        return

    with list_file.open("r", encoding="utf-8") as fh:
        lines = fh.readlines()

    for raw in lines:
        name_raw = raw.strip()
        if not name_raw or name_raw.startswith("#"):
            continue

        # allow entries that include extensions; use stem
        dataset_base = Path(name_raw).stem
        print(f"Processing dataset: {dataset_base}")

        csv_path = base / f"{dataset_base}.csv"
        yaml_path = base / f"{dataset_base}.yaml"
        yaml_path_alt = base / f"{dataset_base}.yml"

        # CSV
        if csv_path.exists():
            try:
                df = pd.read_csv(csv_path)
                table_name = _sanitize_table_name(dataset_base)
                print(f"Writing CSV to table: {table_name}")
                write_dataframe_to_db(df, table_name)
            except Exception as e:
                print(f"ERROR: Failed to read/write CSV {csv_path}: {e}")
                traceback.print_exc()
        else:
            print(f"WARNING: CSV not found for {dataset_base} at {csv_path}")

        # YAML (.yaml preferred, then .yml)
        chosen_yaml = yaml_path if yaml_path.exists() else (yaml_path_alt if yaml_path_alt.exists() else None)
        if chosen_yaml:
            try:
                with chosen_yaml.open("r", encoding="utf-8") as fh:
                    yaml_obj = yaml.safe_load(fh)
                df_yaml = _yaml_to_dataframe(yaml_obj)
                table_name = _sanitize_table_name(f"{dataset_base}_metadata")
                print(f"Writing YAML to table: {table_name}")
                write_dataframe_to_db(df_yaml, table_name)
            except Exception as e:
                print(f"ERROR: Failed to read/write YAML {chosen_yaml}: {e}")
                traceback.print_exc()
        else:
            print(f"WARNING: YAML not found for {dataset_base} (checked .yaml and .yml)")

if __name__ == "__main__":
    write_datasets_to_db()