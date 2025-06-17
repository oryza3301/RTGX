"""
Data Loader Module
=================
Handles loading data from various sources with robust error handling.
"""

import pandas as pd
import numpy as np
import os
import time
from datetime import datetime
from typing import Dict, Any, Optional, Union, List

class DataLoader:
    """Handles data loading from various sources with robust error handling"""
    
    def __init__(self, spark_session=None, max_sample_size=100000, 
                standardize=True, handle_errors=True):
        """
        Initialize DataLoader
        
        Args:
            spark_session: Optional Spark session for big data operations
            max_sample_size: Maximum rows to sample from large datasets
            standardize: Whether to standardize columns and values
            handle_errors: Whether to handle errors or raise exceptions
        """
        self.spark = spark_session
        self.max_sample_size = max_sample_size
        self.standardize = standardize
        self.handle_errors = handle_errors
        
    def load_csv(self, path, **kwargs):
        """
        Load data from CSV file
        
        Args:
            path: Path to CSV file
            **kwargs: Additional parameters for pandas read_csv
            
        Returns:
            Pandas DataFrame
        """
        try:
            options = {
                'header': 'infer',
                'encoding': 'utf-8',
                'na_values': ['', 'NULL', 'null', 'N/A', '#N/A'],
                'low_memory': False
            }
            options.update(kwargs)
            
            if self.spark:
                # Try with Spark first
                try:
                    spark_df = self.spark.read.csv(path, header=True, inferSchema=True)
                    df = self._sample_if_needed(spark_df)
                except:
                    # Fall back to pandas
                    df = pd.read_csv(path, **options)
            else:
                df = pd.read_csv(path, **options)
            
            if self.standardize:
                df = self._standardize_dataframe(df)
            
            return df
        except Exception as e:
            if self.handle_errors:
                print(f"Error loading CSV {path}: {str(e)}")
                return pd.DataFrame()
            else:
                raise
    
    def load_excel(self, path, sheet_name=None, **kwargs):
        """
        Load data from Excel file
        
        Args:
            path: Path to Excel file
            sheet_name: Specific sheet to load or None for all sheets
            **kwargs: Additional parameters for pandas read_excel
            
        Returns:
            Pandas DataFrame or dict of DataFrames
        """
        try:
            if sheet_name:
                df = pd.read_excel(path, sheet_name=sheet_name, **kwargs)
                if self.standardize:
                    df = self._standardize_dataframe(df)
                return df
            else:
                dfs = pd.read_excel(path, sheet_name=None, **kwargs)
                if self.standardize:
                    dfs = {k: self._standardize_dataframe(v) for k, v in dfs.items()}
                return dfs
        except Exception as e:
            if self.handle_errors:
                print(f"Error loading Excel {path}: {str(e)}")
                return pd.DataFrame() if sheet_name else {}
            else:
                raise
    
    def load_sql(self, query, connection):
        """
        Load data from SQL query
        
        Args:
            query: SQL query string
            connection: Database connection string or object
            
        Returns:
            Pandas DataFrame
        """
        try:
            if self.spark and query.strip().upper().startswith("SELECT"):
                df = self.spark.sql(query).toPandas()
            else:
                df = pd.read_sql(query, connection)
            
            if self.standardize:
                df = self._standardize_dataframe(df)
                
            return df
        except Exception as e:
            if self.handle_errors:
                print(f"Error executing SQL query: {str(e)}")
                return pd.DataFrame()
            else:
                raise
    
    def load_table(self, table_name, connection=None):
        """
        Load data from database table
        
        Args:
            table_name: Name of the table
            connection: Database connection (optional if using Spark)
            
        Returns:
            Pandas DataFrame
        """
        try:
            if self.spark and connection is None:
                spark_df = self.spark.table(table_name)
                df = self._sample_if_needed(spark_df)
            else:
                df = pd.read_sql_table(table_name, connection)
            
            if self.standardize:
                df = self._standardize_dataframe(df)
                
            return df
        except Exception as e:
            if self.handle_errors:
                print(f"Error loading table {table_name}: {str(e)}")
                return pd.DataFrame()
            else:
                raise
    
    def load_from_lakehouse(self, path, format_type="parquet"):
        """
        Load data from lakehouse path
        
        Args:
            path: Lakehouse path (abfss:// or wasbs://)
            format_type: File format (parquet, delta, csv)
            
        Returns:
            Pandas DataFrame
        """
        if not self.spark:
            raise ValueError("Spark session is required to load from lakehouse")
            
        try:
            if format_type.lower() == "parquet":
                spark_df = self.spark.read.load(path)
            elif format_type.lower() == "delta":
                spark_df = self.spark.read.format("delta").load(path)
            elif format_type.lower() == "csv":
                spark_df = self.spark.read.csv(path, header=True, inferSchema=True)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
                
            df = self._sample_if_needed(spark_df)
            
            if self.standardize:
                df = self._standardize_dataframe(df)
                
            return df
        except Exception as e:
            if self.handle_errors:
                print(f"Error loading from lakehouse {path}: {str(e)}")
                return pd.DataFrame()
            else:
                raise
    
    def load_multiple_sources(self, sources):
        """
        Load data from multiple sources
        
        Args:
            sources: Dictionary mapping names to source paths/queries
            
        Returns:
            Dictionary of DataFrames
        """
        dfs = {}
        for name, source in sources.items():
            if isinstance(source, str):
                if source.endswith('.csv'):
                    dfs[name] = self.load_csv(source)
                elif source.endswith(('.xlsx', '.xls')):
                    dfs[name] = self.load_excel(source)
                elif source.startswith(('SELECT', 'select')):
                    dfs[name] = self.load_sql(source, None)
                elif source.startswith('abfss://'):
                    dfs[name] = self.load_from_lakehouse(source)
                else:
                    dfs[name] = self.load_table(source)
            elif isinstance(source, pd.DataFrame):
                dfs[name] = source
        return dfs
    
    def _sample_if_needed(self, spark_df):
        """Sample large Spark DataFrame if needed"""
        count = spark_df.count()
        if count > self.max_sample_size:
            print(f"Sampling {self.max_sample_size:,} rows from {count:,} total rows")
            return spark_df.limit(self.max_sample_size).toPandas()
        else:
            return spark_df.toPandas()
    
    def _standardize_dataframe(self, df):
        """Standardize DataFrame by cleaning column names and values"""
        if df is None or df.empty:
            return df
        
        # Standardize column names (remove spaces, special chars)
        df.columns = [self._clean_column_name(col) for col in df.columns]
        
        # Handle string columns - trim and standardize
        for col in df.select_dtypes(include=['object']):
            if hasattr(df[col], 'str'):
                df[col] = df[col].str.strip()
        
        # Handle mixed types - prevent automatic type conversion issues
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if the column has mixed numeric and string values
                numeric_count = pd.to_numeric(df[col], errors='coerce').notna().sum()
                if 0 < numeric_count < len(df):
                    # Mixed types detected - keep as string to prevent data loss
                    df[col] = df[col].astype(str)
            
        return df
    
    def _clean_column_name(self, name):
        """Clean column name while preserving meaning"""
        # Don't convert to lowercase to preserve meaning
        # Just remove trailing/leading spaces
        return str(name).strip()
    
    def generate_sample_data(self, n_tables=3, n_rows=50, include_issues=True):
        """
        Generate sample datasets for testing
        
        Args:
            n_tables: Number of tables to generate
            n_rows: Number of rows per table
            include_issues: Whether to include data quality issues
            
        Returns:
            Dictionary of DataFrames
        """
        # Set seeds for reproducibility
        random.seed(42)
        np.random.seed(42)
        
        # Generate sample tables (customers, orders, products)
        sample_tables = {}
        
        # Customers table
        if n_tables >= 1:
            customers = pd.DataFrame({
                'customer_id': range(1, n_rows + 1),
                'first_name': [random.choice(['John', 'Jane', 'Alice', 'Bob', 'Charlie', None]) 
                              for _ in range(n_rows)],
                'last_name': [random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', None]) 
                             for _ in range(n_rows)],
                'email': [f"user{i}@example.com" if i % 10 != 0 else None for i in range(n_rows)],
                'status': random.choices(['active', 'inactive', 'suspended'], 
                                        weights=[0.7, 0.2, 0.1], k=n_rows)
            })
            sample_tables['customers'] = customers
        
        # Orders table
        if n_tables >= 2:
            orders = pd.DataFrame({
                'order_id': range(1, n_rows * 2 + 1),
                'customer_id': random.choices(range(1, n_rows + 1), k=n_rows * 2),
                'order_date': [datetime.now() - timedelta(days=random.randint(1, 365)) 
                              for _ in range(n_rows * 2)],
                'total': [round(random.uniform(10, 1000), 2) for _ in range(n_rows * 2)],
                'status': random.choices(['pending', 'shipped', 'delivered', 'cancelled'], 
                                        weights=[0.2, 0.3, 0.4, 0.1], k=n_rows * 2)
            })
            sample_tables['orders'] = orders
        
        # Products table
        if n_tables >= 3:
            products = pd.DataFrame({
                'product_id': range(1, n_rows // 2 + 1),
                'name': [f"Product {i}" for i in range(1, n_rows // 2 + 1)],
                'price': [round(random.uniform(5, 500), 2) for _ in range(n_rows // 2)],
                'category': random.choices(['Electronics', 'Clothing', 'Food', 'Books'], 
                                        weights=[0.3, 0.3, 0.2, 0.2], k=n_rows // 2)
            })
            sample_tables['products'] = products
        
        # Add data quality issues if requested
        if include_issues:
            for table_name, df in sample_tables.items():
                # Add missing values
                for col in df.columns:
                    if col.endswith('_id'):  # Skip ID columns
                        continue
                    mask = np.random.choice([True, False], size=len(df), p=[0.05, 0.95])
                    df.loc[mask, col] = None
                
                # Add outliers to numeric columns
                for col in df.select_dtypes(include=['number']).columns:
                    if col.endswith('_id'):  # Skip ID columns
                        continue
                    mask = np.random.choice([True, False], size=len(df), p=[0.03, 0.97])
                    if mask.any():
                        df.loc[mask, col] = df[col].max() * 10
                
                # Add invalid formats
                if 'email' in df.columns:
                    mask = np.random.choice([True, False], size=len(df), p=[0.05, 0.95])
                    df.loc[mask, 'email'] = 'invalid-email'
        
        return sample_tables