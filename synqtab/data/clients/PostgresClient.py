from typing import Any, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from synqtab.environment.postgres import (
    POSTGRES_USER, POSTGRES_PASSWORD,
    POSTGRES_MAPPED_PORT, POSTGRES_HOST, POSTGRES_DB
)
from synqtab.utils.logging_utils import get_logger


LOG = get_logger(__file__)


class SingletonPostgresClient(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonPostgresClient, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class _PostgresClient:
    _engine = create_engine(
        url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_MAPPED_PORT}/{POSTGRES_DB}",
        echo=False,
        pool_pre_ping=True,
    )
    

class PostgresClient(_PostgresClient, metaclass=SingletonPostgresClient):
    
    @classmethod
    def write_dataframe_to_db(
        cls,
        df: pd.DataFrame,
        table_name: str,
        schema: str = "public",
        if_exists: str = "replace",
        index: bool = False,
        chunksize: int = 1000,
        method: Optional[str] = "multi",
    ) -> None:
        try:
            df.to_sql(
                name=table_name,
                con=cls._engine,
                schema=schema,
                if_exists=if_exists,
                index=index,
                method=method,
                chunksize=chunksize,
            )
            LOG.info(f"Wrote {len(df)} rows to {schema}.{table_name}")
        except Exception:
            LOG.exception(f"Failed to write DataFrame to {schema}.{table_name}")
            raise
    
    @classmethod
    def read_table_from_db(
        cls,
        table_name: str,
        schema: str = "public",
        columns: Optional[list] = None,
        index_col: Optional[str] = None,
    ) -> pd.DataFrame:
        try:
            if columns is None:
                df = pd.read_sql_table(table_name, con=cls._engine, schema=schema, index_col=index_col)
            else:
                df = pd.read_sql_table(table_name, con=cls._engine, schema=schema, columns=columns, index_col=index_col)

            LOG.info(f"Read {len(df)} rows from {schema}.{table_name}")
            return df
        except Exception:
            LOG.exception(f"Failed to read table {schema}.{table_name}")
            raise
        
    @classmethod
    def read_table_from_db(
        cls,
        table_name: str,
        schema: str = "public",
        columns: Optional[list] = None,
        index_col: Optional[str] = None,
    ) -> pd.DataFrame:
        try:
            if columns is None:
                df = pd.read_sql_table(table_name, con=cls._engine, schema=schema, index_col=index_col)
            else:
                df = pd.read_sql_table(table_name, con=cls._engine, schema=schema, columns=columns, index_col=index_col)

            LOG.info(f"Read {len(df)} rows from {schema}.{table_name}")
            return df
        except Exception:
            LOG.exception(f"Failed to read table {schema}.{table_name}")
            raise
    
    @classmethod
    def write_runtime_error(
        cls,
        experiment_id: str,
        file_path: str,
        error_message: str,
        errors_table_name: str = 'errors'
    ):
        try:
            query = text(f"""
                INSERT INTO {errors_table_name} (experiment_id, file_path, error_message)
                VALUES (:experiment_id, :file_path, :error_message)            
            """)
            with cls._engine.connect() as connection:
                connection.execute(
                    query,
                    {
                        "experiment_id": experiment_id,
                        "file_path": file_path,
                        "error_message": error_message
                    }
                )
                connection.commit()
            LOG.info(f"Wrote runtime error for experiment {experiment_id} in '{errors_table_name}'")
        except Exception as e:
            LOG.exception(f"Failed to write runtime error for experiment {experiment_id}. Error: {e}")
            raise
        
    @classmethod
    def write_evaluation_result(
        cls,
        experiment_id: str,
        first_input_file_path: str,
        second_input_file_path: str,
        evaluation_shortname: str,
        random_seed: str,
        error_type: str,
        error_rate: str,
        result: int | float,
        notes: Optional[dict[str, Any]] = None,
        evaluation_results_table_name: str = 'evaluation_results'
    ):
        try:
            query = text(f"""
                INSERT INTO {evaluation_results_table_name} (
                    experiment_id,
                    first_input_file_path,
                    second_input_file_path,
                    evaluation_shortname,
                    random_seed,
                    error_type,
                    error_rate,
                    result,
                    notes
                )
                VALUES (
                    :experiment_id,
                    :first_input_file_path,
                    :second_input_file_path,
                    :evaluation_shortname,
                    :random_seed,
                    :error_type,
                    :error_rate,
                    :result,
                    :notes
                )            
            """)
            with cls._engine.connect() as connection:
                connection.execute(
                    query,
                    {
                        "experiment_id": experiment_id,
                        "first_input_file_path": first_input_file_path,
                        "second_input_file_path": second_input_file_path,
                        "evaluation_shortname": evaluation_shortname,
                        "random_seed": random_seed,
                        "error_type": error_type,
                        "error_rate": error_rate,
                        "result": result,
                        "notes": notes if notes else None,
                    }
                )
                connection.commit()
            LOG.info(f"Wrote evaluation result for experiment {experiment_id} in '{evaluation_results_table_name}'")
        except Exception as e:
            LOG.exception(
                f"Failed to write evaluation result for experiment {experiment_id}, \
                    type {evaluation_shortname}. Error: {e}"
                )
            raise
    
    @classmethod
    def evaluation_result_exists(
        cls, 
        experiment_id: str, 
        evaluation_results_table_name: str = 'evaluation_results'
    ) -> bool:
        """Checks if an evaluation result with the specific ID experiment ID exists.

        Args:
            experiment_id (str): The experiment ID to check for existence.

        Returns:
            bool: True if it exists, else False
        """
        try:
            query = text(f"""
                SELECT 1 FROM {evaluation_results_table_name} \
                WHERE experiment_id = :experiment_id \
                LIMIT 1 
            """)
            with cls._engine.connect() as connection:
                result = connection.execute(query, {"experiment_id": experiment_id})
                exists = result.scalar() is not None
                LOG.info(f"Checked existence of evaluation {experiment_id}: {exists}")
                return exists 
        except Exception as e:
            LOG.exception(
                f"Failed to check for existence of experiment {experiment_id}. Error: {e}")
            raise
