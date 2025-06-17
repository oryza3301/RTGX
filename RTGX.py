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
from data_lakehouse import save_results_to_lakehouse, publish_to_tables

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

def save_results(validator, output_dir, format="json"):
    """
    Save validation results to files
    
    Args:
        validator: DataGovernance instance with validation results
        output_dir: Directory to save results
        format: Output format (json or csv)
        
    Returns:
        Dictionary with paths to saved files
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract and save results
    results = validator.validation_results
    paths = {}
    
    if format == "json":
        with open(f"{output_dir}/validation_results.json", "w") as f:
            json.dump(validator.format_all_results(), f, indent=2, default=str)
        paths["results"] = f"{output_dir}/validation_results.json"
    else:
        # Save as CSV
        summary_df = pd.DataFrame(validator.get_summary_stats())
        summary_df.to_csv(f"{output_dir}/validation_summary.csv", index=False)
        paths["summary"] = f"{output_dir}/validation_summary.csv"
        
        # Save detailed results
        detailed_df = validator.get_detailed_results()
        if detailed_df is not None:
            detailed_df.to_csv(f"{output_dir}/validation_details.csv", index=False)
            paths["details"] = f"{output_dir}/validation_details.csv"
    
    return paths

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