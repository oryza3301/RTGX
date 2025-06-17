"""
Data Lakehouse Module
====================
Functions for saving and publishing data quality results to lakehouse.
"""

import re
from datetime import datetime

def save_results_to_lakehouse(validator, project_name, spark_session, 
                             lakehouse_path="abfss://DATA_QUALITY_WS@onelake.dfs.fabric.microsoft.com/LH_DATA_QUALITY.Lakehouse/Files", 
                             output_format="parquet"):
    """
    Save validation results to files in the specified lakehouse
    
    Args:
        validator: DataGovernance instance with validation results
        project_name: Name of the project (will be used in folder path)
        spark_session: Spark session for lakehouse operations
        lakehouse_path: Path to the lakehouse where files will be saved
        output_format: Format to save files (parquet, delta)
        
    Returns:
        Dictionary with paths to saved files and timestamp
    """
    # Validate and clean project name
    if not project_name or not isinstance(project_name, str):
        raise ValueError("Project name must be a non-empty string")
    
    # Clean project name for file system path
    project_name_safe = re.sub(r'[^a-zA-Z0-9_]', '_', project_name)
    
    # Create timestamp for folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # New folder structure: {lakehouse_path}/{project_name}/{timestamp}
    output_path = f"{lakehouse_path}/{project_name_safe}/{timestamp}"
    
    # Extract validation results
    summary_stats = validator.get_summary_stats()
    detailed_results = validator.get_detailed_results()
    
    # Convert to Spark DataFrames and save
    paths = {
        "timestamp": timestamp, 
        "project_name": project_name,
        "project_name_safe": project_name_safe,
        "base_path": f"{lakehouse_path}/{project_name_safe}"
    }
    
    try:
        print(f"Saving results to project folder: {project_name_safe}")
        print(f"Timestamp folder: {timestamp}")
        
        # Save summary statistics
        if summary_stats:
            summary_df = spark_session.createDataFrame(summary_stats)
            summary_path = f"{output_path}/validation_summary"
            summary_df.write.format(output_format).mode("overwrite").save(summary_path)
            paths["summary"] = summary_path
        
        # Save detailed validation results
        if detailed_results is not None:
            detailed_spark_df = spark_session.createDataFrame(detailed_results)
            details_path = f"{output_path}/validation_details"
            detailed_spark_df.write.format(output_format).mode("overwrite").save(details_path)
            paths["details"] = details_path
        
        # Save per-table validation results
        formatted_results = validator.format_all_results()
        for table_name, result in formatted_results.items():
            # Convert result to DataFrame format
            result_rows = []
            for expectation in result.get("results", []):
                expectation_config = expectation.get("expectation_config", {})
                row = {
                    "table_name": table_name,
                    "expectation_type": expectation_config.get("type", ""),
                    "column": expectation_config.get("kwargs", {}).get("column", ""),
                    "success": expectation.get("success", False),
                    "parameters": str(expectation_config.get("kwargs", {})),
                    "details": str(expectation.get("result", {})),
                    "timestamp": timestamp
                }
                result_rows.append(row)
            
            if result_rows:
                table_result_df = spark_session.createDataFrame(result_rows)
                table_path = f"{output_path}/{table_name}_results"
                table_result_df.write.format(output_format).mode("overwrite").save(table_path)
                paths[f"{table_name}_results"] = table_path
        
        print(f"✅ Saved validation results to {output_path}")
        
    except Exception as e:
        print(f"❌ Error saving results to lakehouse: {str(e)}")
        raise
    
    return paths

def publish_to_tables(paths, project_name, spark_session, 
                     target_database="LH_DATA_QUALITY", 
                     specific_timestamp=None):
    """
    Publish saved files as tables in the lakehouse
    
    Args:
        paths: Dictionary with paths to files
        project_name: Name of the project (used for table naming)
        spark_session: Spark session for lakehouse operations
        target_database: Database where tables will be created
        specific_timestamp: Optional specific timestamp to publish
        
    Returns:
        List of published table names
    """
    # Validate project name
    if not project_name or not isinstance(project_name, str):
        raise ValueError("Project name must be a non-empty string")
    
    # Get the safe project name from paths or create it
    project_name_safe = paths.get("project_name_safe", re.sub(r'[^a-zA-Z0-9_]', '_', project_name))
    
    published_tables = []
    
    # Filter paths by timestamp if specified
    if specific_timestamp:
        filtered_paths = {}
        base_path = paths.get("base_path", "")
        
        if base_path:
            # Construct paths for the specific timestamp
            specific_folder = f"{base_path}/{specific_timestamp}"
            
            # Reconstruct paths for specific timestamp
            for key, path in paths.items():
                if key in ["timestamp", "project_name", "project_name_safe", "base_path"]:
                    continue
                    
                # Extract the file name from the original path
                file_name = path.split('/')[-1]
                new_path = f"{specific_folder}/{file_name}"
                filtered_paths[key] = new_path
        else:
            # Fallback: filter by timestamp in path
            for key, path in paths.items():
                if key in ["timestamp", "project_name", "project_name_safe", "base_path"]:
                    continue
                if specific_timestamp in path:
                    filtered_paths[key] = path
        
        if not filtered_paths:
            print(f"❌ No paths found with timestamp {specific_timestamp}")
            return []
        
        paths_to_publish = filtered_paths
        print(f"📅 Publishing specific timestamp: {specific_timestamp}")
    else:
        # Skip the metadata entries
        paths_to_publish = {k: v for k, v in paths.items() 
                           if k not in ["timestamp", "project_name", "project_name_safe", "base_path"]}
        current_timestamp = paths.get("timestamp", "current")
        print(f"📅 Publishing latest timestamp: {current_timestamp}")
    
    # Use only the first part of the database name (no dots)
    if '.' in target_database:
        clean_db_name = target_database.split('.')[0]
    else:
        clean_db_name = target_database
    
    print(f"🗄️  Using database: {clean_db_name}")
    print(f"📁 Project folder: {project_name_safe}")
    
    # Publish each path as a table
    for file_type, path in paths_to_publish.items():
        try:
            # Create table name from project and file type
            table_name = f"{project_name_safe}_{file_type}"
            
            # Read the data - use parquet format since that's how we saved it
            print(f"📖 Reading from path: {path}")
            df = spark_session.read.format("parquet").load(path)
            
            # Check if we actually got data
            if df is None:
                print(f"❌ Error: No data found at path {path}")
                continue
                
            row_count = df.count()
            print(f"📊 Loaded {row_count} rows for {file_type}")
            
            # Write directly to the table
            full_table_name = f"{clean_db_name}.{table_name}"
            print(f"💾 Writing to table: {full_table_name}")
            
            # Simple direct write with overwrite
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
            
            published_tables.append(table_name)
            print(f"✅ Successfully published table: {table_name}")
            
        except Exception as e:
            print(f"❌ Error publishing {file_type}: {str(e)}")
            
            # Try alternative approach with temp view
            try:
                print("🔄 Attempting alternative approach with temporary view...")
                
                # Make sure df is defined before using it
                if 'df' not in locals() or df is None:
                    print(f"📖 Re-reading data from {path}")
                    df = spark_session.read.format("parquet").load(path)
                
                temp_view_name = f"temp_view_{file_type}_{int(time.time())}"
                df.createOrReplaceTempView(temp_view_name)
                
                # Use the database
                spark_session.sql(f"USE {clean_db_name}")
                
                # Drop table if exists and create new one
                spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
                spark_session.sql(f"CREATE TABLE {table_name} USING DELTA AS SELECT * FROM {temp_view_name}")
                
                # Clean up temp view
                spark_session.sql(f"DROP VIEW {temp_view_name}")
                
                published_tables.append(table_name)
                print(f"✅ Successfully published table using alternative approach: {table_name}")
                
            except Exception as e2:
                print(f"❌ Alternative approach also failed: {str(e2)}")
                print(f"⏭️  Skipping {file_type}")
    
    print(f"🎉 Published {len(published_tables)} tables successfully")
    return published_tables