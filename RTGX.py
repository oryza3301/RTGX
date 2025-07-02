"""
RTGX - Data Governance Library
======================
A comprehensive library for data profiling, quality validation, and governance for RiceTec's internal functions
"""

import pandas as pd
import numpy as np
import os
import re
import json
import datetime
import time
from datetime import datetime, timedelta
import random
import great_expectations as gx
from typing import Dict, List, Any, Optional, Union

# Import sub-modules
from data_loader import DataLoader
from data_analyzer import DatabaseOverview
from data_validator import DataGovernance
from data_lakehouse import save_results_to_lakehouse, publish_results_to_tables

from pyspark.sql.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType, ShortType, ByteType, DecimalType
)
from pyspark.sql import DataFrame

__version__ = "1.0.0"

# Simplified API for common operations
def load_data(source, **kwargs):
    """
    Load data from various sources (CSV, Excel, SQL, Lakehouse)
    
    Args:
        source: Path, SQL query, or DataFrame
        **kwargs: Additional loading parameters
        
    Returns:
        DataLoader instance with loaded data
    """
    loader = DataLoader(**kwargs)
    
    if isinstance(source, str):
        if source.endswith('.csv'):
            return loader.load_csv(source)
        elif source.endswith(('.xlsx', '.xls')):
            return loader.load_excel(source)
        elif source.startswith(('SELECT', 'select')):
            return loader.load_sql(source, kwargs.get('connection'))
        elif source.startswith('abfss://'):
            return loader.load_from_lakehouse(source)
        else:
            return loader.load_table(source)
    elif isinstance(source, dict):
        return source  # Already a dict of DataFrames
    elif isinstance(source, pd.DataFrame):
        return {"data": source}  # Convert single DataFrame to dict
    else:
        raise ValueError(f"Unsupported data source type: {type(source)}")

def analyze_data(datasets):
    """
    Analyze database structure for the provided datasets
    
    Args:
        datasets: Dictionary of pandas DataFrames or single DataFrame
        
    Returns:
        DatabaseOverview instance with analysis results
    """
    if isinstance(datasets, pd.DataFrame):
        datasets = {"data": datasets}
        
    analyzer = DatabaseOverview(datasets)
    analyzer.analyze()
    return analyzer

def validate_data(datasets, rules=None, primary_keys=None):
    """
    Validate data using auto-generated rules and/or custom rules
    
    Args:
        datasets: Dictionary of pandas DataFrames or single DataFrame
        rules: Optional dictionary of validation rules by table
        primary_keys: Optional dictionary of primary key definitions by table
                     Can be single column, list of columns, or composite string like 'col1' + 'col2'
        
    Returns:
        DataGovernance instance with validation results
    """
    if isinstance(datasets, pd.DataFrame):
        datasets = {"data": datasets}
        
    validator = DataGovernance()
    validator.register_datasets(datasets)
    
    # Add base validations with primary keys if provided
    for table in datasets:
        pk = None
        if primary_keys and table in primary_keys:
            pk = primary_keys[table]
        validator.add_base_validations(table, primary_key=pk)
    
    # Add custom rules if provided
    if rules:
        for table, table_rules in rules.items():
            if table in datasets:
                validator.add_rules_from_config(table, table_rules)
    
    # Run validation
    results = validator.run_all_validations()
    return validator

# (Keep your existing load_data, analyze_data, validate_data functions)

def save_validation_results(validator, project_name, subproject_id, spark_session, lakehouse_path):
    """
    Saves the complete validation results to a timestamped folder in the lakehouse.

    Args:
        validator: The DataGovernance instance after running validations.
        project_name (str): The user-defined name of the project.
        subproject_id (str): The user-defined name of the subproject.
        spark_session: Active Spark session.
        lakehouse_path: Base path in the lakehouse.

    Returns:
        Dictionary with run details including the generated timestamp.
    """
    from data_lakehouse import save_results_to_lakehouse
    
    print("\n--- Saving Validation Results to Lakehouse ---")
    run_details = save_results_to_lakehouse(
        validator=validator,
        project_name=project_name,
        subproject_id=subproject_id,
        spark_session=spark_session,
        lakehouse_path=lakehouse_path
    )
    print("--- Save Complete ---")
    return run_details

def publish_run_to_master_tables(project_name, subproject_id, timestamp, spark_session, lakehouse_path, target_database):
    """
    Publishes (appends) the results of a specific run to the master Delta tables.
    
    Args:
        project_name (str): The name of the project.
        subproject_id (str): The name of the subproject.
        timestamp (str): The timestamp folder of the run to publish.
        spark_session: Active Spark session.
        lakehouse_path: Base path where results are stored.
        target_database (str): The database for the master tables.
    """
    from data_lakehouse import publish_results_to_tables
    
    print(f"\n--- Publishing Run {timestamp} to Master Tables ---")
    publish_results_to_tables(
        project_name=project_name,
        subproject_id=subproject_id,
        timestamp=timestamp,
        spark_session=spark_session,
        lakehouse_path=lakehouse_path,
        target_database=target_database
    )
    print("--- Publication Complete ---")

def remove_run_from_master_tables(project_name, subproject_id, execution_iso_timestamp, spark_session, target_database):
    """
    Deletes all records associated with a specific run from the master tables.

    Args:
        project_name (str): The name of the project to delete from.
        subproject_id (str): The name of the subproject to delete from.
        execution_iso_timestamp (str): The ISO-formatted execution timestamp to remove.
        spark_session: Active Spark session.
        target_database (str): The database containing the master tables.
    """
    from data_lakehouse import delete_data_by_timestamp
    
    print(f"\n--- Deleting Run {execution_iso_timestamp} from Master Tables ---")
    delete_data_by_timestamp(
        project_name=project_name,
        subproject_id=subproject_id,
        timestamp_to_delete=execution_iso_timestamp,
        spark_session=spark_session,
        target_database=target_database
    )
    print("--- Deletion Complete ---")

def export_rules_summary(validator, output_path="rules_summary.xlsx"):
    """
    Export validation rules summary to Excel
    
    Args:
        validator: DataGovernance instance with configured rules
        output_path: Path where to save the Excel file
        
    Returns:
        Path to the saved Excel file
    """
    return validator.export_rules_summary(output_path)

def to_pandas(spark_df: DataFrame, table_name: str):
    # Use toPandas() directly for the initial conversion
    pandas_df = spark_df.toPandas()
    # Get the original schema from Spark to use as the source of truth
    spark_schema = spark_df.schema
    # Define a mapping from Spark types to modern, nullable Pandas dtypes
    type_mapping = {
        StringType: "string",
        IntegerType: "Int32",
        LongType: "Int64",
        ShortType: "Int16",
        ByteType: "Int8",
        DoubleType: "float64",
        FloatType: "float32",
        BooleanType: "boolean",
        DateType: "datetime64[ns]",
        TimestampType: "datetime64[ns]",
        DecimalType: "float64"  # Convert Decimal to float for compatibility
    }
 
    # Loop through each column defined in the Spark schema and apply the correct type
    for field in spark_schema:
        col_name = field.name
        spark_type = type(field.dataType)
        target_pandas_type = type_mapping.get(spark_type)
 
        if target_pandas_type:
            try:
                # Only change the type if it's not already correct
                if str(pandas_df[col_name].dtype) != target_pandas_type:
                    pandas_df[col_name] = pandas_df[col_name].astype(target_pandas_type)
            except (ValueError, TypeError):
                # If conversion fails (e.g., trying to make a string 'abc' an integer),
                # just skip it and let it be logged in the verification step.
                pass
 
    # --- Verification Step ---
    mismatched_columns = []
    for col in pandas_df.columns:
        spark_col_type_str = str(spark_schema[col].dataType)
        pandas_col_type_str = str(pandas_df[col].dtype)
        # Check if the final type matches the intended mapping
        spark_type = type(spark_schema[col].dataType)
        target_type = type_mapping.get(spark_type)
        # Log if there is a mismatch
        if target_type and pandas_col_type_str != target_type:
             mismatched_columns.append({
                "table_name": table_name,
                "column": col,
                "spark_data_type": spark_col_type_str,
                "pandas_data_type": pandas_col_type_str
            })
 
    verification_df = pd.DataFrame(mismatched_columns)
    return pandas_df, verification_df
