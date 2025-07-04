"""
Data Lakehouse Module
====================
Functions for saving and publishing data quality results to lakehouse.
"""

import re
from datetime import datetime
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, LongType

def _calculate_quality_score(summary_df, detailed_issues_df):
    """
    Internal function to calculate the Quality Score based on cell-level errors.
    """
    if detailed_issues_df.empty:
        summary_df['quality_score'] = 100.0
        return summary_df

    # Calculate total cells from the summary dataframe
    total_cells = summary_df['row_count'].sum() * len(summary_df.columns)
    total_failing_cells = len(detailed_issues_df['row_num'].unique())

    if total_cells == 0:
        quality_score = 0.0
    else:
        # Score is based on the percentage of non-failing cells
        quality_score = ((total_cells - total_failing_cells) / total_cells) * 100
        quality_score = max(0, quality_score) # Ensure score is not negative

    summary_df['quality_score'] = quality_score
    return summary_df

def save_results_to_lakehouse(validator, project_name, subproject_id, spark_session, lakehouse_path):
    """
    Saves all validation results to a timestamped folder in the lakehouse.
    """
    project_name_safe = re.sub(r'[^a-zA-Z0-9_]', '_', project_name)
    subproject_id_safe = re.sub(r'[^a-zA-Z0-9_]', '_', subproject_id)
    timestamp_folder = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{lakehouse_path}/{project_name_safe}/{subproject_id_safe}/{timestamp_folder}"

    # Generate all dataframes from the validator
    summary_df = pd.DataFrame(validator.get_summary_stats(project_name, subproject_id))
    detailed_rules_df = validator.get_detailed_results(project_name, subproject_id)
    issues_outliers_df = validator.get_issues_and_outliers(project_name, subproject_id)
    detailed_issues_df = validator.get_detailed_cell_level_issues(project_name, subproject_id)

    # Recalculate the Quality Score using the local function
    summary_df = _calculate_quality_score(summary_df, detailed_issues_df)

    try:
        if not summary_df.empty:
            spark_session.createDataFrame(summary_df).write.format("parquet").mode("overwrite").save(f"{output_path}/project_summary")
            print(f"✅ Saved Project Summary to {output_path}/project_summary")

        if not detailed_rules_df.empty:
            spark_session.createDataFrame(detailed_rules_df).write.format("parquet").mode("overwrite").save(f"{output_path}/detailed_rules")
            print(f"✅ Saved Detailed Rules to {output_path}/detailed_rules")

        if not issues_outliers_df.empty:
            spark_session.createDataFrame(issues_outliers_df).write.format("parquet").mode("overwrite").save(f"{output_path}/issues_outliers")
            print(f"✅ Saved Issues & Outliers to {output_path}/issues_outliers")

        if not detailed_issues_df.empty:
            # Schema definition to prevent Parquet write errors
            detailed_issues_schema = StructType([
                StructField("project_id", StringType(), True), StructField("subproject_id", StringType(), True),
                StructField("table_name", StringType(), True), StructField("column_name", StringType(), True),
                StructField("row_num", LongType(), True), StructField("timestamp", StringType(), True),
                StructField("value", StringType(), True), StructField("violation_not_null", StringType(), True),
                StructField("violation_data_type", StringType(), True), StructField("violation_in_set", StringType(), True),
                StructField("violation_regex", StringType(), True), StructField("violation_range", StringType(), True),
                StructField("violation_unique", StringType(), True)
            ])
            # Convert all columns (except row_num) to string to ensure safe writing
            for col in detailed_issues_df.columns:
                if col != 'row_num':
                    detailed_issues_df[col] = detailed_issues_df[col].astype(str).replace('nan', None)

            detailed_issues_spark_df = spark_session.createDataFrame(detailed_issues_df, schema=detailed_issues_schema)
            detailed_issues_spark_df.write.format("parquet").mode("overwrite").save(f"{output_path}/detailed_issues")
            print(f"✅ Saved Detailed Issues to {output_path}/detailed_issues")

    except Exception as e:
        print(f"❌ Error saving results to lakehouse: {str(e)}")
        raise
        
    return {"timestamp": timestamp_folder}

# Keep the other functions (publish_results_to_tables, etc.) as they were.

def publish_results_to_tables(project_name, subproject_id, timestamp, spark_session, lakehouse_path, target_database):
    """
    Publishes data from a specific run to the final Delta tables.
    - Appends to Project_Summary, Detailed_Rules, and Issues_Outliers.
    - OVERWRITES the Detailed_Issues table for a fresh snapshot every time.
    """
    project_name_safe = re.sub(r'[^a-zA-Z0-9_]', '_', project_name)
    subproject_id_safe = re.sub(r'[^a-zA-Z0-9_]', '_', subproject_id)
    source_path_base = f"{lakehouse_path}/{project_name_safe}/{subproject_id_safe}/{timestamp}"
    
    # Define source paths and target tables
    publication_map = {
        "project_summary": "Project_Summary",
        "detailed_rules": "Detailed_Rules",
        "issues_outliers": "Issues_Outliers",
        "detailed_issues": "Detailed_Issues"
    }

    print(f"📅 Publishing results for '{project_name} - {subproject_id}' from timestamp: {timestamp}")
    
    for source_file, target_table_name in publication_map.items():
        source_parquet_path = f"{source_path_base}/{source_file}"
        full_table_name = f"{target_database}.{target_table_name}"
        
        try:
            print(f"📖 Reading from path: {source_parquet_path}")
            df_to_publish = spark_session.read.format("parquet").load(source_parquet_path)

            if df_to_publish.rdd.isEmpty():
                print(f"⚠️ No data found at {source_parquet_path}. Skipping table '{full_table_name}'.")
                continue

            # --- THE CORE LOGIC CHANGE IS HERE ---
            if target_table_name == "Detailed_Issues":
                # For the detailed issues table, always overwrite
                write_mode = "overwrite"
                print(f"💾 OVERWRITING {df_to_publish.count()} rows in table: {full_table_name}")
            else:
                # For all other tables, append to maintain history
                write_mode = "append"
                print(f"💾 Appending {df_to_publish.count()} rows to table: {full_table_name}")
            
            # Write to the delta table with the determined mode
            df_to_publish.write.format("delta").mode(write_mode).option("mergeSchema", "true").saveAsTable(full_table_name)
            
            print(f"✅ Successfully published to table: {full_table_name} (Mode: {write_mode})")

        except Exception as e:
            if "Path does not exist" in str(e):
                 print(f"⚠️ Source path not found: {source_parquet_path}. Skipping.")
            else:
                print(f"❌ Error publishing to {full_table_name}: {str(e)}")

def delete_data_by_timestamp(project_name, subproject_id, execution_iso_timestamp, spark_session, target_database):
    """
    Deletes all data associated with a specific run from all master tables.
    """
    if not all([project_name, subproject_id, execution_iso_timestamp]):
        raise ValueError("Project name, subproject ID, and timestamp must be provided.")
        
    tables_to_clean = ["Project_Summary", "Detailed_Rules", "Issues_Outliers", "Detailed_Issues"] # New table
    
    print(f"🗑️  Attempting to delete data for '{project_name} - {subproject_id}' with timestamp '{execution_iso_timestamp}'...")
    
    for table in tables_to_clean:
        full_table_name = f"{target_database}.{table}"
        try:
            from delta.tables import DeltaTable
            delta_table = DeltaTable.forName(spark_session, full_table_name)
            
            condition = f"project_id = '{project_name}' AND subproject_id = '{subproject_id}' AND timestamp = '{execution_iso_timestamp}'"
            print(f"Executing DELETE on {full_table_name} with condition: {condition}")
            
            delta_table.delete(condition=condition)
            print(f"✅ Successfully deleted data from {full_table_name}.")
            
            print(f"VACCUMing table {full_table_name}...")
            delta_table.vacuum()

        except Exception as e:
            print(f"❌ Error deleting data from {full_table_name}: {str(e)}")
