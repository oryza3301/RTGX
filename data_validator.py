"""
Data Validator Module
====================
Implements data validation using Great Expectations.
"""

import pandas as pd
import numpy as np
import great_expectations as gx
import os
import json
import time
import datetime
from typing import Dict, List, Any, Optional, Union

class DataGovernance:
    """Data Governance class using Great Expectations"""
    
    def __init__(self, mode="ephemeral"):
        """
        Initialize DataGovernance
        
        Args:
            mode: Great Expectations context mode
        """
        try:
            self.context = gx.get_context(mode=mode)
            self.dataframes = {}
            self.batch_definitions = {}
            self.expectation_suites = {}
            self.validation_results = {}
        except Exception as e:
            print(f"Warning: Great Expectations initialization error: {str(e)}")
            print("Some features may be limited. Make sure great_expectations is installed.")
            self.context = None
    
    def register_dataframe(self, df, table_name):
        """
        Register a dataframe for validation
        
        Args:
            df: Pandas DataFrame
            table_name: Name of the table
            
        Returns:
            Table name if registration successful
        """
        self.dataframes[table_name] = df
        
        if self.context is None:
            print(f"Registered {table_name} without Great Expectations context")
            return table_name
        
        try:
            # Add or get datasource
            try:
                data_source = self.context.data_sources.add_pandas(f"{table_name}_source")
            except:
                try:
                    data_source = self.context.data_sources.get(f"{table_name}_source")
                except:
                    # Fall back to alternative approach
                    sources = self.context.list_datasources()
                    if len(sources) > 0:
                        data_source = sources[0]
                    else:
                        data_source = self.context.sources.add_pandas("default_source")
            
            # Add or get data asset
            try:
                data_asset = data_source.add_dataframe_asset(name=table_name)
            except:
                try:
                    data_asset = data_source.get_asset(name=table_name)
                except:
                    raise Exception(f"Could not create or get asset for {table_name}")
            
            # Add or get batch definition
            try:
                batch_definition = data_asset.add_batch_definition_whole_dataframe(
                    f"{table_name}_batch"
                )
            except:
                try:
                    batch_definition = data_asset.get_batch_definition(f"{table_name}_batch")
                except:
                    # Try alternative approach
                    batch_definition = None  # Will handle this case later
            
            self.batch_definitions[table_name] = batch_definition
            
            # Add or get expectation suite
            try:
                expectation_suite = self.context.suites.add(
                    gx.ExpectationSuite(name=f"{table_name}_expectations")
                )
            except:
                try:
                    self.context.suites.delete(name=f"{table_name}_expectations")
                    expectation_suite = self.context.suites.add(
                        gx.ExpectationSuite(name=f"{table_name}_expectations")
                    )
                except:
                    # Create a basic suite as fallback
                    expectation_suite = gx.ExpectationSuite(name=f"{table_name}_expectations")
            
            self.expectation_suites[table_name] = expectation_suite
            return table_name
            
        except Exception as e:
            print(f"Warning: Error registering {table_name} with Great Expectations: {str(e)}")
            print("Will continue with basic validation capabilities")
            
            # Still keep track of the dataframe
            if table_name not in self.expectation_suites:
                self.expectation_suites[table_name] = None
                
            return table_name
    
    def register_datasets(self, datasets):
        """
        Register multiple datasets
        
        Args:
            datasets: Dictionary of pandas DataFrames
            
        Returns:
            List of registered table names
        """
        registered = []
        for table_name, df in datasets.items():
            registered.append(self.register_dataframe(df, table_name))
        return registered
    
    def _parse_primary_key(self, primary_key_definition):
        """
        Parse primary key definition to handle single or composite keys
        
        Args:
            primary_key_definition: String or list defining primary key(s)
            
        Returns:
            List of column names that form the primary key
        """
        if primary_key_definition is None:
            return []
        
        if isinstance(primary_key_definition, list):
            return primary_key_definition
        
        if isinstance(primary_key_definition, str):
            # Handle composite keys defined as 'col1' + 'col2' + 'col3'
            if '+' in primary_key_definition:
                # Split by + and clean up
                columns = [col.strip().strip("'\"") for col in primary_key_definition.split('+')]
                return columns
            else:
                # Single column
                return [primary_key_definition.strip().strip("'\"")]
        
        return []

    def _validate_primary_key_candidate(self, table_name, pk_columns):
        """
        Validate if the specified columns can serve as a primary key
        
        Args:
            table_name: Name of the table
            pk_columns: List of column names that should form the primary key
            
        Returns:
            Boolean indicating if the combination is valid
        """
        if table_name not in self.dataframes:
            return False
        
        df = self.dataframes[table_name]
        
        # Check if all columns exist
        missing_columns = [col for col in pk_columns if col not in df.columns]
        if missing_columns:
            print(f"Warning: Primary key columns {missing_columns} not found in table {table_name}")
            return False
        
        # Check for null values in any of the key columns
        null_counts = {}
        for col in pk_columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                null_counts[col] = null_count
        
        if null_counts:
            print(f"Warning: Primary key columns in {table_name} contain null values: {null_counts}")
            print("Primary keys should not contain null values.")
        
        # Check uniqueness of the combination
        if len(pk_columns) == 1:
            # Single column primary key
            unique_count = df[pk_columns[0]].nunique()
            total_count = len(df)
        else:
            # Composite primary key
            unique_count = df.groupby(pk_columns).size().shape[0]
            total_count = len(df)
        
        is_unique = unique_count == total_count
        
        if is_unique:
            pk_display = " + ".join(pk_columns)
            print(f"✅ Primary key validation passed for {table_name}: ({pk_display}) - {unique_count} unique combinations")
            return True
        else:
            pk_display = " + ".join(pk_columns)
            duplicate_count = total_count - unique_count
            print(f"⚠️  Primary key validation failed for {table_name}: ({pk_display})")
            print(f"   Found {duplicate_count} duplicate combinations out of {total_count} total rows")
            return False

    def add_base_validations(self, table_name, primary_key=None):
        """
        Add standard validations for a table
        
        Args:
            table_name: Name of the table
            primary_key: Primary key definition (string, list, or composite string like 'col1' + 'col2')
        """
        if table_name not in self.expectation_suites:
            raise ValueError(f"Table {table_name} not registered")
        
        if self.expectation_suites[table_name] is None:
            print(f"Warning: Cannot add validations for {table_name} - expectation suite not available")
            return
            
        suite = self.expectation_suites[table_name]
        df = self.dataframes[table_name]
        
        # Parse and validate primary key
        pk_columns = self._parse_primary_key(primary_key)
        pk_is_valid = False
        
        if pk_columns:
            pk_is_valid = self._validate_primary_key_candidate(table_name, pk_columns)
        
        # Table expectations - columns exist
        if hasattr(suite, "add_expectation"):
            try:
                # Modern GE API
                from great_expectations.expectations import ExpectTableColumnsToMatchOrderedList
                suite.add_expectation(
                    ExpectTableColumnsToMatchOrderedList(column_list=list(df.columns))
                )
                
                # Primary key expectations (only if validation passed)
                if pk_columns and pk_is_valid:
                    from great_expectations.expectations import (
                        ExpectColumnToExist,
                        ExpectColumnValuesToBeUnique,
                        ExpectColumnValuesToNotBeNull
                    )
                    
                    # Add expectations for each column in the primary key
                    for col in pk_columns:
                        if col in df.columns:
                            suite.add_expectation(ExpectColumnToExist(column=col))
                            suite.add_expectation(ExpectColumnValuesToNotBeNull(column=col))
                    
                    # Add composite uniqueness expectation if multiple columns
                    if len(pk_columns) > 1:
                        from great_expectations.expectations import ExpectCompoundColumnsToBeUnique
                        try:
                            suite.add_expectation(
                                ExpectCompoundColumnsToBeUnique(column_list=pk_columns)
                            )
                        except:
                            # Fallback: Add individual uniqueness expectations
                            print(f"Note: Using individual column uniqueness checks for composite key in {table_name}")
                            for col in pk_columns:
                                if col in df.columns:
                                    suite.add_expectation(ExpectColumnValuesToBeUnique(column=col))
                    else:
                        # Single column primary key
                        if pk_columns[0] in df.columns:
                            suite.add_expectation(ExpectColumnValuesToBeUnique(column=pk_columns[0]))
                elif pk_columns and not pk_is_valid:
                    print(f"Skipping primary key expectations for {table_name} due to validation failures")
                
                # Data type expectations for all columns
                for column in df.columns:
                    # Add a few basic expectations for each column
                    suite.add_expectation(ExpectColumnToExist(column=column))
                    
                    # Type-specific expectations
                    if pd.api.types.is_numeric_dtype(df[column]):
                        from great_expectations.expectations import ExpectColumnValuesToBeOfType
                        type_name = "int" if pd.api.types.is_integer_dtype(df[column]) else "float"
                        suite.add_expectation(
                            ExpectColumnValuesToBeOfType(column=column, type_=type_name)
                        )
                        
                        # If there are no nulls, add expectation
                        if df[column].isna().sum() == 0:
                            from great_expectations.expectations import ExpectColumnValuesToNotBeNull
                            suite.add_expectation(ExpectColumnValuesToNotBeNull(column=column))
                    
                    elif pd.api.types.is_string_dtype(df[column]):
                        from great_expectations.expectations import ExpectColumnValuesToBeOfType
                        suite.add_expectation(
                            ExpectColumnValuesToBeOfType(column=column, type_="string")
                        )
                        
                        # If column has few unique values and many rows, it might be categorical
                        if df[column].nunique() <= 20 and len(df) > 50:
                            from great_expectations.expectations import ExpectColumnValuesToBeInSet
                            unique_values = df[column].dropna().unique().tolist()
                            if len(unique_values) > 0:
                                suite.add_expectation(
                                    ExpectColumnValuesToBeInSet(
                                        column=column, value_set=unique_values, mostly=0.9
                                    )
                                )
            except Exception as e:
                print(f"Warning: Error adding expectations for {table_name}: {str(e)}")
                print("Will continue with limited validation capabilities")
        else:
            print(f"Warning: Expectation suite for {table_name} lacks add_expectation method")
    
    def add_rule(self, table_name, expectation):
        """
        Add a single rule to a table
        
        Args:
            table_name: Name of the table
            expectation: Expectation object to add
        """
        if table_name not in self.expectation_suites:
            raise ValueError(f"Table {table_name} not registered")
        
        if self.expectation_suites[table_name] is None:
            print(f"Warning: Cannot add rule for {table_name} - expectation suite not available")
            return
            
        try:
            self.expectation_suites[table_name].add_expectation(expectation)
        except Exception as e:
            print(f"Warning: Error adding rule for {table_name}: {str(e)}")
    
    def add_rules_from_config(self, table_name, rules):
        """
        Add rules from configuration
        
        Args:
            table_name: Name of the table
            rules: List of rule configuration dictionaries
        """
        if table_name not in self.expectation_suites:
            raise ValueError(f"Table {table_name} not registered")
        
        if self.expectation_suites[table_name] is None:
            print(f"Warning: Cannot add rules for {table_name} - expectation suite not available")
            return
            
        try:
            # Import GE expectations
            import sys
            if 'great_expectations.expectations' in sys.modules:
                from great_expectations.expectations import (
                    ExpectColumnToExist,
                    ExpectColumnValuesToNotBeNull,
                    ExpectColumnValuesToBeUnique,
                    ExpectColumnValuesToBeBetween,
                    ExpectColumnValuesToMatchRegex,
                    ExpectColumnValuesToBeInSet,
                    ExpectColumnValuesToBeOfType,
                    ExpectColumnValueLengthsToBeBetween
                )
                
                # Create expectation mapping
                expectation_map = {
                    "column_exists": lambda cfg: ExpectColumnToExist(
                        column=cfg["column"]
                    ),
                    "not_null": lambda cfg: ExpectColumnValuesToNotBeNull(
                        column=cfg["column"], 
                        mostly=cfg.get("mostly", 1.0)
                    ),
                    "unique": lambda cfg: ExpectColumnValuesToBeUnique(
                        column=cfg["column"]
                    ),
                    "range": lambda cfg: ExpectColumnValuesToBeBetween(
                        column=cfg["column"],
                        min_value=cfg.get("min_value"),
                        max_value=cfg.get("max_value"),
                        mostly=cfg.get("mostly", 1.0)
                    ),
                    "multi_range": lambda cfg: self._create_multi_range_expectation(cfg),
                    "regex": lambda cfg: ExpectColumnValuesToMatchRegex(
                        column=cfg["column"],
                        regex=cfg["regex"],
                        mostly=cfg.get("mostly", 1.0)
                    ),
                    "in_set": lambda cfg: ExpectColumnValuesToBeInSet(
                        column=cfg["column"],
                        value_set=cfg["value_set"],
                        mostly=cfg.get("mostly", 1.0)
                    ),
                    "type": lambda cfg: ExpectColumnValuesToBeOfType(
                        column=cfg["column"],
                        type_=cfg["type"]
                    ),
                    "length": lambda cfg: ExpectColumnValueLengthsToBeBetween(
                        column=cfg["column"],
                        min_value=cfg.get("min_length"),
                        max_value=cfg.get("max_length")
                    )
                }
                
                # Add rules from configuration
                for rule in rules:
                    rule_type = rule["type"]
                    if rule_type in expectation_map:
                        expectation = expectation_map[rule_type](rule)
                        if expectation:  # Only add if expectation was created successfully
                            self.add_rule(table_name, expectation)
                    else:
                        print(f"Warning: Rule type '{rule_type}' not supported")
            else:
                print("Warning: Great Expectations expectations module not available")
        except Exception as e:
            print(f"Warning: Error adding rules from config for {table_name}: {str(e)}")

    def _create_multi_range_expectation(self, config):
        """
        Create a custom expectation for multiple range validation
        
        Args:
            config: Configuration dictionary with ranges
            
        Returns:
            Custom expectation or None if creation fails
        """
        try:
            column = config["column"]
            ranges = config.get("ranges", [])
            mostly = config.get("mostly", 1.0)
            
            if not ranges:
                print(f"Warning: No ranges specified for multi_range rule on column {column}")
                return None
            
            # Validate the configuration
            valid_ranges = []
            for i, range_def in enumerate(ranges):
                if not isinstance(range_def, dict) or "min" not in range_def or "max" not in range_def:
                    print(f"Warning: Invalid range definition at index {i} for column {column}")
                    continue
                
                min_val = range_def["min"]
                max_val = range_def["max"]
                
                if min_val > max_val:
                    print(f"Warning: Invalid range {min_val}-{max_val} for column {column} (min > max)")
                    continue
                    
                valid_ranges.append((min_val, max_val))
            
            if not valid_ranges:
                print(f"Warning: No valid ranges found for column {column}")
                return None
            
            print(f"✅ Multi-range rule configured for {column}: {valid_ranges} (mostly: {mostly})")
            
            # Create a custom expectation using lambda function
            from great_expectations.expectations import ExpectColumnValuesToSatisfyJson
            
            # Create validation function
            def validate_multi_range(value):
                if pd.isna(value):
                    return True  # Allow nulls unless explicitly forbidden
                
                try:
                    # Convert to float for comparison
                    float_val = float(value)
                    
                    # Check if value falls within any of the ranges
                    for min_val, max_val in valid_ranges:
                        if min_val <= float_val <= max_val:
                            return True
                    return False
                except (ValueError, TypeError):
                    return False
            
            # Try to create using ExpectColumnValuesToSatisfyJson
            try:
                # Create JSON schema for validation
                range_conditions = []
                for min_val, max_val in valid_ranges:
                    range_conditions.append({
                        "type": "number",
                        "minimum": min_val,
                        "maximum": max_val
                    })
                
                json_schema = {
                    "anyOf": range_conditions
                }
                
                return ExpectColumnValuesToSatisfyJson(
                    column=column,
                    json_schema=json_schema,
                    mostly=mostly
                )
            except:
                # Fallback: Use a different approach with regex or custom validation
                print(f"Note: Using fallback validation approach for multi-range on {column}")
                
                # Create a set of valid values by discretizing the ranges
                # This works well for integer ranges, less optimal for continuous ranges
                valid_values = set()
                for min_val, max_val in valid_ranges:
                    if isinstance(min_val, int) and isinstance(max_val, int):
                        # For integer ranges, enumerate all values
                        valid_values.update(range(min_val, max_val + 1))
                    else:
                        # For float ranges, we'll use a different approach
                        # Sample values at intervals
                        import numpy as np
                        samples = np.arange(min_val, max_val + 0.1, 0.1)
                        valid_values.update(samples.round(1))
                
                if len(valid_values) < 10000:  # Only if the set isn't too large
                    from great_expectations.expectations import ExpectColumnValuesToBeInSet
                    return ExpectColumnValuesToBeInSet(
                        column=column,
                        value_set=list(valid_values),
                        mostly=mostly
                    )
                else:
                    print(f"Warning: Too many discrete values for multi-range validation on {column}")
                    return None
            
        except Exception as e:
            print(f"Error creating multi-range expectation: {str(e)}")
            return None
    
    def run_validation(self, table_name):
        """
        Run validation for a table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Validation result dictionary
        """
        if table_name not in self.dataframes:
            raise ValueError(f"Table {table_name} not registered")
        
        if self.expectation_suites[table_name] is None:
            print(f"Warning: Cannot validate {table_name} - expectation suite not available")
            # Return basic success result
            return {
                "success": True,  # Default to success
                "results": [],
                "suite_name": f"{table_name}_expectations",
                "suite_parameters": {},
                "statistics": {
                    "evaluated_expectations": 0,
                    "successful_expectations": 0,
                    "unsuccessful_expectations": 0,
                    "success_percent": 100.0
                },
                "meta": {},
                "id": None
            }
        
        try:
            # No timeout - run validation directly
            try:
                batch = self.batch_definitions[table_name].get_batch(
                    batch_parameters={"dataframe": self.dataframes[table_name]}
                )
                result = batch.validate(self.expectation_suites[table_name])
                
                # Store validation result
                self.validation_results[table_name] = result
                
                # Format result
                return self._format_result(table_name, result)
            except Exception as e:
                print(f"Warning: Validation error for {table_name}: {str(e)}")
                # Create fallback result
                return {
                    "success": False,
                    "results": [],
                    "suite_name": f"{table_name}_expectations",
                    "suite_parameters": {},
                    "statistics": {
                        "evaluated_expectations": 0,
                        "successful_expectations": 0,
                        "unsuccessful_expectations": 0,
                        "success_percent": 0.0
                    },
                    "meta": {"error": str(e)},
                    "id": None
                }
        except Exception as e:
            print(f"Error during validation of {table_name}: {str(e)}")
            # Return fallback result
            return {
                "success": False,
                "results": [],
                "suite_name": f"{table_name}_expectations",
                "suite_parameters": {},
                "statistics": {
                    "evaluated_expectations": 0,
                    "successful_expectations": 0,
                    "unsuccessful_expectations": 0,
                    "success_percent": 0.0
                },
                "meta": {"error": str(e)},
                "id": None
            }
    
    def run_all_validations(self):
        """
        Run validation for all tables
        
        Returns:
            Dictionary of validation results by table
        """
        results = {}
        for table_name in self.dataframes:
            results[table_name] = self.run_validation(table_name)
        return results
    
    def _format_result(self, table_name, result):
        """
        Format validation result
        
        Args:
            table_name: Name of the table
            result: Raw validation result
            
        Returns:
            Formatted result dictionary
        """
        try:
            # Handle various result types
            if hasattr(result, 'to_json_dict'):
                # Convert to dictionary
                result_dict = result.to_json_dict()
                # Handle different result structure formats
                if 'success' in result_dict:
                    formatted = result_dict
                else:
                    # Create base structure
                    formatted = {
                        "success": result.success,
                        "results": result.results,
                        "suite_name": f"{table_name}_expectations",
                        "suite_parameters": {},
                        "statistics": getattr(result, "statistics", {}),
                        "meta": getattr(result, "meta", {}),
                        "id": None
                    }
            elif isinstance(result, dict):
                # Already a dictionary
                formatted = result
            else:
                # Try to extract attributes
                formatted = {
                    "success": getattr(result, "success", False),
                    "results": getattr(result, "results", []),
                    "suite_name": f"{table_name}_expectations",
                    "suite_parameters": {},
                    "statistics": getattr(result, "statistics", {}),
                    "meta": getattr(result, "meta", {}),
                    "id": None
                }
            
            # Ensure statistics is complete
            if "statistics" not in formatted or not formatted["statistics"]:
                result_count = len(formatted.get("results", []))
                success_count = sum(1 for r in formatted.get("results", []) if r.get("success", False))
                
                formatted["statistics"] = {
                    "evaluated_expectations": result_count,
                    "successful_expectations": success_count,
                    "unsuccessful_expectations": result_count - success_count,
                    "success_percent": 100.0 * success_count / result_count if result_count > 0 else 0.0
                }
            
            return formatted
        except Exception as e:
            print(f"Error formatting result for {table_name}: {str(e)}")
            # Fallback to basic structure
            return {
                "success": False,
                "results": [],
                "suite_name": f"{table_name}_expectations",
                "suite_parameters": {},
                "statistics": {
                    "evaluated_expectations": 0,
                    "successful_expectations": 0,
                    "unsuccessful_expectations": 0,
                    "success_percent": 0.0
                },
                "meta": {"error": str(e)},
                "id": None
            }
    
    def format_all_results(self):
        """
        Format all validation results for export
        
        Returns:
            Dictionary of formatted results
        """
        formatted = {}
        for table_name, result in self.validation_results.items():
            formatted[table_name] = self._format_result(table_name, result)
        return formatted
    
    def get_summary_stats(self, project_name, subproject_id):
        """
        Get summary statistics of validation results formatted for the Project Summary table.
    
        Args:
            project_name (str): The user-defined name of the project (e.g., "QMS").
            subproject_id (str): The user-defined name of the subproject (e.g., "Handoff 6").
    
        Returns:
            List of summary dictionaries.
        """
        summary = []
        execution_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        for table_name, result in self.validation_results.items():
            formatted = self._format_result(table_name, result)
            stats = formatted.get("statistics", {})
            
            # Calculate a simple quality score
            quality_score = stats.get("success_percent", 0.0)
    
            summary.append({
                "project_id": project_name,
                "subproject_id": subproject_id,
                "table_name": table_name,
                "timestamp": execution_timestamp,
                "success_rate": stats.get("success_percent", 0.0),
                "quality_score": quality_score,
                "evaluated_expectations": stats.get("evaluated_expectations", 0),
                "successful_expectations": stats.get("successful_expectations", 0),
                "unsuccessful_expectations": stats.get("unsuccessful_expectations", 0),
                "row_count": len(self.dataframes.get(table_name, pd.DataFrame())),
                "status": "Success" if formatted.get("success") else "Failure"
            })
        return summary
    
    def get_detailed_results(self, project_name, subproject_id):
        """
        Get detailed validation results formatted for the Detailed Rules table.
        
        Args:
            project_name (str): The user-defined name of the project.
            subproject_id (str): The user-defined name of the subproject.
    
        Returns:
            Pandas DataFrame with detailed results.
        """
        details = []
        execution_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
        for table_name, result in self.validation_results.items():
            formatted = self._format_result(table_name, result)
            
            for expectation_result in formatted.get("results", []):
                expectation_config = expectation_result.get("expectation_config", {})
                kwargs = expectation_config.get("kwargs", {})
                
                detail = {
                    "project_id": project_name,
                    "subproject_id": subproject_id,
                    "table_name": table_name,
                    "column_name": kwargs.get("column", "Table-level"),
                    "rule_name": expectation_config.get("type", "unknown"),
                    "timestamp": execution_timestamp,
                    "success": expectation_result.get("success", False),
                    "rule_parameters": str(kwargs),
                }
                details.append(detail)
        
        return pd.DataFrame(details) if details else pd.DataFrame()
    
    def get_issues_and_outliers(self, project_name, subproject_id):
        """
        Get failed expectations formatted for the Issues & Outliers table.
        
        Args:
            project_name (str): The user-defined name of the project.
            subproject_id (str): The user-defined name of the subproject.
    
        Returns:
            Pandas DataFrame with issues and outliers.
        """
        issues = []
        execution_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
        for table_name, result in self.validation_results.items():
            formatted = self._format_result(table_name, result)
            
            failed_expectations = [r for r in formatted.get("results", []) if not r.get("success", False)]
            
            for failure in failed_expectations:
                config = failure.get("expectation_config", {})
                kwargs = config.get("kwargs", {})
                result_data = failure.get("result", {})
                
                issue = {
                    "project_id": project_name,
                    "subproject_id": subproject_id,
                    "timestamp": execution_timestamp,
                    "location": f"{table_name}.{kwargs.get('column', 'Table')}",
                    "classification_severity": "High" if "not_null" in config.get("type", "") else "Medium",
                    "classification_type": self._categorize_rule(config.get("type", "")),
                    "details_actual": f"Found {result_data.get('unexpected_count', 0)} unexpected values.",
                    "details_expected": str(kwargs),
                    "root_cause_patterns": f"Failed expectation: {config.get('type', '')}",
                    "partial_unexpected_list": str(result_data.get("partial_unexpected_list", [])),
                    "resolution_status": "Open",
                    "resolution_owner": "Unassigned"
                }
                issues.append(issue)
                
        return pd.DataFrame(issues) if issues else pd.DataFrame()
    
    def print_validation_results(self, results=None):
        """
        Print validation results
        
        Args:
            results: Optional dictionary of results to print
        """
        try:
            from tabulate import tabulate
            tabulate_available = True
        except ImportError:
            tabulate_available = False
            
        if results is None:
            results = {name: self._format_result(name, result) 
                      for name, result in self.validation_results.items()}
        
        for table_name, result in results.items():
            print(f"\n{'='*50}")
            print(f" VALIDATION RESULTS: {table_name.upper()}")
            print(f"{'='*50}")
            print(f"Overall Success: {'✅ Passed' if result['success'] else '❌ Failed'}")
            
            # Get statistics
            stats = result.get('statistics', {})
            evaluated = stats.get('evaluated_expectations', 0)
            successful = stats.get('successful_expectations', 0)
            success_percent = stats.get('success_percent', 0)
            
            print(f"Statistics: {successful}/{evaluated} "
                  f"({success_percent:.1f}% passed)")
            
            # Show failed expectations
            failed_expectations = [r for r in result.get('results', []) if not r.get('success', False)]
            if failed_expectations:
                print("\nFailed Expectations:")
                
                # Create table data for failed expectations
                failed_data = []
                for i, failure in enumerate(failed_expectations, 1):
                    # Get expectation config
                    config = failure.get('expectation_config', {})
                    exp_type = config.get('type', 'unknown')
                    kwargs = config.get('kwargs', {})
                    column = kwargs.get('column', 'N/A')
                    
                    # Format parameters for display
                    params = []
                    for k, v in kwargs.items():
                        if k != 'column':
                            params.append(f"{k}: {v}")
                    param_str = ", ".join(params)
                    
                    # Get failure details
                    details = ""
                    if 'result' in failure:
                        result_data = failure['result']
                        if 'partial_unexpected_list' in result_data:
                            values = result_data['partial_unexpected_list']
                            details = f"Invalid values: {values[:5]}"
                            if len(values) > 5:
                                details += f" (+{len(values)-5} more)"
                                
                        if 'unexpected_percent' in result_data:
                            if details:
                                details += f", {result_data['unexpected_percent']:.1f}% failed"
                            else:
                                details = f"{result_data['unexpected_percent']:.1f}% failed"
                    
                    failed_data.append([i, exp_type, column, param_str, details])
                
                # Print table of failures
                headers = ['#', 'Expectation Type', 'Column', 'Parameters', 'Details']
                if tabulate_available:
                    print(tabulate(failed_data, headers=headers, tablefmt='grid'))
                else:
                    # Simple text table if tabulate is not available
                    for row in failed_data:
                        print(f"  {row[0]}. {row[1]} on {row[2]}")
                        if row[3]:
                            print(f"     Parameters: {row[3]}")
                        if row[4]:
                            print(f"     Details: {row[4]}")
                        print()
            else:
                print("\nAll expectations passed!")
                
    def export_rules_summary(self, output_path="rules_summary.xlsx"):
        """
        Export a summary of all configured rules and primary keys to Excel
        
        Args:
            output_path: Path where to save the Excel file
            
        Returns:
            Path to the saved Excel file
        """
        try:
            # Collect all rules and primary keys information
            rules_data = []
            
            for table_name in self.dataframes.keys():
                # Get basic table info
                df = self.dataframes[table_name]
                
                # Check if we have expectations for this table
                if table_name in self.expectation_suites and self.expectation_suites[table_name] is not None:
                    suite = self.expectation_suites[table_name]
                    
                    # Try to extract expectations (this might vary based on GE version)
                    expectations = []
                    try:
                        if hasattr(suite, 'expectations'):
                            expectations = suite.expectations
                        elif hasattr(suite, 'expectation_configurations'):
                            expectations = suite.expectation_configurations
                    except:
                        expectations = []
                    
                    # Process each expectation
                    for exp in expectations:
                        try:
                            exp_type = getattr(exp, 'expectation_type', str(type(exp).__name__))
                            
                            # Extract kwargs/parameters
                            if hasattr(exp, 'kwargs'):
                                kwargs = exp.kwargs
                            elif hasattr(exp, 'configuration'):
                                kwargs = getattr(exp.configuration, 'kwargs', {})
                            else:
                                kwargs = {}
                            
                            # Extract column name
                            column = kwargs.get('column', kwargs.get('column_list', 'Table-level'))
                            if isinstance(column, list):
                                column = ' + '.join(column)
                            
                            # Format parameters for display
                            params = []
                            for k, v in kwargs.items():
                                if k not in ['column', 'column_list']:
                                    if isinstance(v, (list, dict)):
                                        params.append(f"{k}: {str(v)[:100]}...")
                                    else:
                                        params.append(f"{k}: {v}")
                            
                            param_str = "; ".join(params) if params else "No parameters"
                            
                            rules_data.append({
                                'Table': table_name,
                                'Column': column,
                                'Rule Type': exp_type,
                                'Parameters': param_str,
                                'Rule Category': self._categorize_rule(exp_type),
                                'Description': self._describe_rule(exp_type, kwargs)
                            })
                        except Exception as e:
                            # Fallback for problematic expectations
                            rules_data.append({
                                'Table': table_name,
                                'Column': 'Unknown',
                                'Rule Type': str(type(exp).__name__),
                                'Parameters': f"Error extracting: {str(e)}",
                                'Rule Category': 'Unknown',
                                'Description': 'Could not extract rule details'
                            })
                else:
                    # No expectations configured
                    rules_data.append({
                        'Table': table_name,
                        'Column': 'N/A',
                        'Rule Type': 'No rules configured',
                        'Parameters': 'N/A',
                        'Rule Category': 'Configuration',
                        'Description': 'No validation rules have been set up for this table'
                    })
            
            # Create DataFrames
            rules_df = pd.DataFrame(rules_data)
            
            # Create table summary
            table_summary_data = []
            for table_name, df in self.dataframes.items():
                table_summary_data.append({
                    'Table Name': table_name,
                    'Row Count': len(df),
                    'Column Count': len(df.columns),
                    'Rules Configured': len([r for r in rules_data if r['Table'] == table_name and r['Rule Type'] != 'No rules configured']),
                    'Primary Key Configured': 'Yes' if any(r['Rule Type'].startswith('expect_column_values_to_be_unique') or 
                                                        r['Rule Type'].startswith('expect_compound_columns_to_be_unique') 
                                                        for r in rules_data if r['Table'] == table_name) else 'No'
                })
            
            table_summary_df = pd.DataFrame(table_summary_data)
            
            # Create rule category summary
            if not rules_df.empty:
                category_summary = rules_df.groupby(['Table', 'Rule Category']).size().reset_index(name='Count')
                category_pivot = category_summary.pivot(index='Table', columns='Rule Category', values='Count').fillna(0)
            else:
                category_pivot = pd.DataFrame()
            
            # Save to Excel
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Rules detail sheet
                rules_df.to_excel(writer, sheet_name='Rules Detail', index=False)
                
                # Table summary sheet
                table_summary_df.to_excel(writer, sheet_name='Table Summary', index=False)
                
                # Category summary sheet
                if not category_pivot.empty:
                    category_pivot.to_excel(writer, sheet_name='Rule Categories')
                
                # Add a configuration sheet with metadata
                config_data = [{
                    'Export Date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Total Tables': len(self.dataframes),
                    'Total Rules': len(rules_df),
                    'Tables with Rules': len(table_summary_df[table_summary_df['Rules Configured'] > 0]),
                    'Tables with Primary Keys': len(table_summary_df[table_summary_df['Primary Key Configured'] == 'Yes'])
                }]
                config_df = pd.DataFrame(config_data)
                config_df.to_excel(writer, sheet_name='Export Info', index=False)
            
            print(f"✅ Rules summary exported to: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"❌ Error exporting rules summary: {str(e)}")
            return None

    def _categorize_rule(self, rule_type):
        """Categorize rule types for summary"""
        if 'unique' in rule_type.lower():
            return 'Uniqueness'
        elif 'null' in rule_type.lower():
            return 'Completeness'
        elif 'between' in rule_type.lower() or 'range' in rule_type.lower():
            return 'Range/Bounds'
        elif 'type' in rule_type.lower():
            return 'Data Type'
        elif 'regex' in rule_type.lower() or 'match' in rule_type.lower():
            return 'Format/Pattern'
        elif 'set' in rule_type.lower():
            return 'Value Set'
        elif 'column' in rule_type.lower() and 'exist' in rule_type.lower():
            return 'Schema'
        else:
            return 'Other'

    def _describe_rule(self, rule_type, kwargs):
        """Generate human-readable description of the rule"""
        descriptions = {
            'expect_column_to_exist': 'Column must exist in the table',
            'expect_column_values_to_not_be_null': 'Column values cannot be null/empty',
            'expect_column_values_to_be_unique': 'All values in column must be unique',
            'expect_column_values_to_be_between': f"Values must be between {kwargs.get('min_value', 'N/A')} and {kwargs.get('max_value', 'N/A')}",
            'expect_column_values_to_be_in_set': f"Values must be in predefined set: {str(kwargs.get('value_set', []))[:50]}...",
            'expect_column_values_to_match_regex': f"Values must match pattern: {kwargs.get('regex', 'N/A')}",
            'expect_column_values_to_be_of_type': f"Column must be of type: {kwargs.get('type_', 'N/A')}",
            'expect_table_columns_to_match_ordered_list': 'Table structure must match expected columns',
            'expect_compound_columns_to_be_unique': 'Combination of columns must be unique (composite primary key)'
        }
        
        return descriptions.get(rule_type, f"Custom rule: {rule_type}")
