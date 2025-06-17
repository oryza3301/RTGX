"""
Data Lakehouse Module
====================
Functions for saving and publishing data quality results to lakehouse.
"""

import re
from datetime import datetime
import pandas as pd

def save_results_to_lakehouse(validator, project_name, subproject_id, spark_session, lakehouse_path):
    """
    Save validation results into a timestamped folder in the lakehouse.
    The folder structure will be {lakehouse_path}/{project_name}/{subproject_id}/{timestamp}.
    
    Args:
        validator: DataGovernance instance with validation results.
        project_name (str): The user-defined name of the project (e.g., "QMS").
        subproject_id (str): The user-defined name of the subproject (e.g., "Handoff 6").
        spark_session: Active Spark session.
        lakehouse_path: Base path in the lakehouse for storing results.
        
    Returns:
        Dictionary with paths to the saved files and the timestamp.
    """
    if not project_name or not subproject_id:
        raise ValueError("Project name and subproject ID must be non-empty strings")
    
    project_name_safe = re.sub(r'[^a-zA-Z0-9_]', '_', project_name)
    subproject_id_safe = re.sub(r'[^a-zA-Z0-9_]', '_', subproject_id)
    timestamp_folder = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    output_path = f"{lakehouse_path}/{project_name_safe}/{subproject_id_safe}/{timestamp_folder}"
    
    print(f"Saving results to: {output_path}")
    
    # Generate the three dataframes
    project_summary_data = validator.get_summary_stats(project_name, subproject_id)
    detailed_rules_df = validator.get_detailed_results(project_name, subproject_id)
    issues_outliers_df = validator.get_issues_and_outliers(project_name, subproject_id)

    saved_paths = {
        "timestamp": timestamp_folder,
        "project_name": project_name,
        "subproject_id": subproject_id,
        "base_path": output_path
    }

    try:
        # 1. Save Project Summary
        if project_summary_data:
            summary_spark_df = spark_session.createDataFrame(pd.DataFrame(project_summary_data))
            summary_path = f"{output_path}/project_summary"
            summary_spark_df.write.format("parquet").mode("overwrite").save(summary_path)
            saved_paths["project_summary"] = summary_path
            print(f"✅ Saved Project Summary to {summary_path}")

        # 2. Save Detailed Rules
        if not detailed_rules_df.empty:
            detailed_spark_df = spark_session.createDataFrame(detailed_rules_df)
            details_path = f"{output_path}/detailed_rules"
            detailed_spark_df.write.format("parquet").mode("overwrite").save(details_path)
            saved_paths["detailed_rules"] = details_path
            print(f"✅ Saved Detailed Rules to {details_path}")

        # 3. Save Issues and Outliers
        if not issues_outliers_df.empty:
            issues_spark_df = spark_session.createDataFrame(issues_outliers_df)
            issues_path = f"{output_path}/issues_outliers"
            issues_spark_df.write.format("parquet").mode("overwrite").save(issues_path)
            saved_paths["issues_outliers"] = issues_path
            print(f"✅ Saved Issues & Outliers to {issues_path}")

    except Exception as e:
        print(f"❌ Error saving results to lakehouse: {str(e)}")
        raise
    
    return saved_paths

def publish_results_to_tables(project_name, subproject_id, timestamp, spark_session, lakehouse_path, target_database):
    """
    Publishes (appends) data from a specific timestamped run to the final Delta tables.

    Args:
        project_name (str): The name of the project.
        subproject_id (str): The name of the subproject.
        timestamp (str): The timestamp folder to publish from (e.g., "20250530_104439").
        spark_session: Active Spark session.
        lakehouse_path: Base path where results are stored.
        target_database (str): The target database for the final tables.
    """
    project_name_safe = re.sub(r'[^a-zA-Z0-9_]', '_', project_name)
    subproject_id_safe = re.sub(r'[^a-zA-Z0-9_]', '_', subproject_id)
    source_path_base = f"{lakehouse_path}/{project_name_safe}/{subproject_id_safe}/{timestamp}"
    
    publication_map = {
        "project_summary": "Project_Summary",
        "detailed_rules": "Detailed_Rules",
        "issues_outliers": "Issues_Outliers"
    }

    print(f"📅 Publishing results for '{project_name} - {subproject_id}' from timestamp: {timestamp}")
    
    for source_file, target_table_name in publication_map.items():
        source_parquet_path = f"{source_path_base}/{source_file}"
        full_table_name = f"{target_database}.{target_table_name}"
        
        try:
            print(f"📖 Reading from path: {source_parquet_path}")
            df_to_append = spark_session.read.format("parquet").load(source_parquet_path)

            if df_to_append.rdd.isEmpty():
                print(f"⚠️ No data found at {source_parquet_path}. Skipping table '{full_table_name}'.")
                continue

            print(f"💾 Appending {df_to_append.count()} rows to table: {full_table_name}")
            df_to_append.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(full_table_name)
            print(f"✅ Successfully appended to table: {full_table_name}")

        except Exception as e:
            if "Path does not exist" in str(e):
                 print(f"⚠️ Source path not found: {source_parquet_path}. Skipping.")
            else:
                print(f"❌ Error publishing to {full_table_name}: {str(e)}")

def delete_data_by_timestamp(project_name, subproject_id, execution_iso_timestamp, spark_session, target_database):
    """
    Deletes all data associated with a specific project, subproject, and timestamp from the master tables.

    Args:
        project_name (str): The project name to filter by.
        subproject_id (str): The subproject name to filter by.
        execution_iso_timestamp (str): The ISO format UTC timestamp string to delete.
        spark_session: Active Spark session.
        target_database (str): The database containing the tables.
    """
    if not all([project_name, subproject_id, execution_iso_timestamp]):
        raise ValueError("Project name, subproject ID, and timestamp must be provided.")
        
    tables_to_clean = ["Project_Summary", "Detailed_Rules", "Issues_Outliers"]
    
    print(f"🗑️  Attempting to delete data for '{project_name} - {subproject_id}' with timestamp '{execution_iso_timestamp}'...")
    
    for table in tables_to_clean:
        full_table_name = f"{target_database}.{table}"
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forName(spark_session, full_table_name)
            
            # More specific delete condition
            condition = f"project_id = '{project_name}' AND subproject_id = '{subproject_id}' AND timestamp = '{execution_iso_timestamp}'"
            print(f"Executing DELETE on {full_table_name} with condition: {condition}")
            
            result = delta_table.delete(condition=condition)
            
            print(f"✅ Successfully deleted data from {full_table_name}.")
            
            print(f"VACCUMing table {full_table_name} to clean up old files...")
            delta_table.vacuum()

        except Exception as e:
            print(f"❌ Error deleting data from {full_table_name}: {str(e)}")
