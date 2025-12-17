import os
import pandas as pd
import yaml
from sklearn.preprocessing import LabelEncoder
from db_utils import write_dataframe_to_db

# --- Configuration ---
DATASETS_DIR = "../tabarena_datasets"
DB_SCHEMA = "tabarena_label_encoded"
# -------------------

def process_and_load_datasets():
    """
    Finds dataset pairs (.csv, .yaml) in DATASETS_DIR, processes them,
    and loads them into the database.
    """
    print(f"Starting dataset processing from directory: `{DATASETS_DIR}`")
    try:
        files = os.listdir(DATASETS_DIR)
    except FileNotFoundError:
        print(f"ERROR: Directory not found: `{DATASETS_DIR}`. Please create it and add your datasets.")
        return

    # Find all unique dataset names (without extension)
    dataset_names = sorted(list(set([os.path.splitext(f)[0] for f in files])))

    for name in dataset_names:
        csv_path = os.path.join(DATASETS_DIR, f"{name}.csv")
        yaml_path = os.path.join(DATASETS_DIR, f"{name}.yaml")

        # Check if both files for a dataset exist
        if not (os.path.exists(csv_path) and os.path.exists(yaml_path)):
            print(f"WARNING: Skipping `{name}`: missing corresponding .csv or .yaml file.")
            continue

        print(f"Processing dataset: `{name}`")

        try:
            # 1. Read YAML to get metadata and categorical features
            with open(yaml_path, 'r') as f:
                metadata = yaml.safe_load(f)

            categorical_features = metadata.get('categorical_features', [])
            if not categorical_features:
                print(f"WARNING: No `categorical_features` found in yaml for `{name}`.")

            if metadata.get('problem_type', None) == 'classification':
                categorical_features.append(metadata.get('target_feature', ''))

            # 2. Read CSV data
            df = pd.read_csv(csv_path)

            # 3. Label Encode categorical features
            if categorical_features:
                le = LabelEncoder()
                for col in categorical_features:
                    if col in df.columns:
                        # Ensure column is of string type before encoding to handle mixed types
                        df[col] = le.fit_transform(df[col].astype(str))
                    else:
                        print(f"ERROR: Column `{col}` from yaml not found in csv for `{name}`.")

            if name == 'Marketing_Campaign':
                timestamp_column = 'Dt_Customer'
                if timestamp_column in df.columns:
                    # Convert to datetime, then to integer (Unix timestamp in seconds)
                    # Using errors='coerce' will turn unparseable dates into NaT (Not a Time)
                    dt_series = pd.to_datetime(df[timestamp_column], format='%Y-%m-%d', errors='coerce')
                    # Convert to Unix timestamp (seconds) and fill NaNs with a placeholder like 0
                    df[timestamp_column] = (dt_series.astype('int64') // 10 ** 9).fillna(0).astype(int)

            if name == 'QSAR-TID-11':
                # Convert binary columns to integer, keep MEDIAN_PXC50 as float
                for col in df.columns:
                    if col != 'MEDIAN_PXC50':
                        unique_values = df[col].dropna().unique()
                        if set(unique_values).issubset({0.0, 1.0}):
                            df[col] = df[col].astype('int8')

                # Round MEDIAN_PXC50 to reduce precision
                if 'MEDIAN_PXC50' in df.columns:
                    df['MEDIAN_PXC50'] = df['MEDIAN_PXC50'].round(6)

            # 4. Write processed DataFrame to DB
            # Sanitize table name for SQL
            table_name = name.replace('-', '_').replace(' ', '_').lower()
            print(f"Writing data to table `{DB_SCHEMA}.{table_name}`...")
            write_dataframe_to_db(df, table_name=table_name, schema=DB_SCHEMA, row_by_row=True)

            # 5. Write YAML metadata to a separate DB table
            meta_df = pd.DataFrame(list(metadata.items()), columns=['meta_key', 'meta_value'])
            # Convert lists/dicts in meta_value to strings for DB compatibility
            meta_df['meta_value'] = meta_df['meta_value'].apply(str)
            meta_table_name = f"{table_name}_meta"
            print(f"Writing metadata to table `{DB_SCHEMA}.{meta_table_name}`...")
            write_dataframe_to_db(meta_df, table_name=meta_table_name, schema=DB_SCHEMA)

        except Exception as e:
            print(f"ERROR: Failed to process and load dataset `{name}`. Error: {e}")

    print("Finished all datasets.")

if __name__ == "__main__":
    # Note: Ensure your .env file is configured with your database credentials.
    # The script assumes a schema named 'tabarena' exists in your database.
    # You might need to create it first: CREATE SCHEMA tabarena;
    process_and_load_datasets()