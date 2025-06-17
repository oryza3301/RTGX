"""
Data Analyzer Module
===================
Provides comprehensive database structure analysis.
"""

import pandas as pd
import numpy as np
import re
from typing import Dict, List, Any, Optional, Union

class DatabaseOverview:
    """Analyzes database structure and provides summaries"""
    
    def __init__(self, datasets=None):
        """
        Initialize with optional datasets
        
        Args:
            datasets: Dictionary of pandas DataFrames or single DataFrame
        """
        if isinstance(datasets, pd.DataFrame):
            self.datasets = {"data": datasets}
        else:
            self.datasets = datasets or {}
        self.summary = None
    
    def load_datasets(self, datasets):
        """
        Load datasets for analysis
        
        Args:
            datasets: Dictionary of pandas DataFrames or single DataFrame
            
        Returns:
            Self for method chaining
        """
        if isinstance(datasets, pd.DataFrame):
            self.datasets = {"data": datasets}
        else:
            self.datasets = datasets or {}
        return self
    
    def _analyze_table(self, table_name, df):
        """
        Analyze table structure
        
        Args:
            table_name: Name of the table
            df: Pandas DataFrame
            
        Returns:
            Dictionary with table analysis
        """
        # Detect primary keys
        primary_keys = self._detect_primary_keys(df)
        
        # Get basic table stats
        stats = {
            'table_name': table_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'primary_keys': primary_keys,
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2),
            'has_nulls': df.isna().any().any(),
            'duplicate_rows': len(df) - len(df.drop_duplicates()),
        }
        
        # Detect common table patterns
        if 'created_at' in df.columns or 'updated_at' in df.columns:
            stats['has_audit_columns'] = True
        
        return stats
    
    def analyze(self):
        """
        Perform comprehensive database analysis
        
        Returns:
            Dictionary with analysis results
        """
        table_summaries = []
        column_summaries = []
        relationships = []
        
        # Analyze each table
        for table_name, df in self.datasets.items():
            # Pass original row count if available
            table_info = self._analyze_table(table_name, df)
            table_summaries.append(table_info)
            
            # Analyze columns
            for column in df.columns:
                col_info = self._analyze_column(df[column], column, table_name, table_info['primary_keys'])
                column_summaries.append(col_info)
        
        # Find relationships
        relationships = self._find_relationships()
        
        self.summary = {
            'table_summaries': pd.DataFrame(table_summaries),
            'column_summaries': pd.DataFrame(column_summaries),
            'relationships': relationships
        }
        
        return self.summary
    
    def _analyze_column(self, series, column_name, table_name, primary_keys):
        """
        Analyze column properties
        
        Args:
            series: Pandas Series (column)
            column_name: Name of the column
            table_name: Name of the table
            primary_keys: List of primary key columns
            
        Returns:
            Dictionary with column analysis
        """
        # Handle any data type safely
        col_info = {
            'table_name': table_name,
            'column_name': column_name,
            'data_type': str(series.dtype),
            'is_unique': series.nunique() == len(series),
            'is_primary_key': column_name in primary_keys,
            'null_count': series.isna().sum(),
            'null_percentage': round(series.isna().sum() / len(series) * 100, 2) if len(series) > 0 else 0,
            'unique_count': series.nunique(),
            'unique_percentage': round(series.nunique() / len(series) * 100, 2) if len(series) > 0 else 0,
        }
        
        # Type-specific analysis
        if pd.api.types.is_numeric_dtype(series):
            col_info.update(self._analyze_numeric(series))
        elif pd.api.types.is_datetime64_dtype(series):
            col_info.update(self._analyze_datetime(series))
        else:
            col_info.update(self._analyze_categorical_or_text(series))
        
        # Detect common column patterns
        col_info.update(self._detect_column_patterns(series, column_name))
        
        return col_info
    
    def _analyze_numeric(self, series):
        """Analyze numeric column"""
        # Handle potential errors with consistent output
        try:
            non_null = series.dropna()
            if len(non_null) == 0:
                return {
                    'range_type': 'numeric',
                    'min_value': None,
                    'max_value': None,
                    'mean_value': None,
                    'median_value': None,
                    'std_dev': None,
                    'data_range': None,
                }
            
            stats = {
                'range_type': 'numeric',
                'min_value': float(non_null.min()),
                'max_value': float(non_null.max()),
                'mean_value': float(non_null.mean()),
                'median_value': float(non_null.median()),
                'std_dev': float(non_null.std()),
                'has_negatives': (non_null < 0).any(),
                'has_zeros': (non_null == 0).any(),
                'data_range': f"{float(non_null.min())} to {float(non_null.max())}",
            }
            
            # Detect outliers using IQR
            q1 = non_null.quantile(0.25)
            q3 = non_null.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            outliers = non_null[(non_null < lower_bound) | (non_null > upper_bound)]
            stats['outlier_count'] = len(outliers)
            stats['outlier_percentage'] = round(len(outliers) / len(non_null) * 100, 2) if len(non_null) > 0 else 0
            
            return stats
        except Exception as e:
            return {
                'range_type': 'numeric',
                'min_value': None,
                'max_value': None,
                'mean_value': None,
                'data_range': f"Error: {str(e)}",
            }
    
    def _analyze_datetime(self, series):
        """Analyze datetime column"""
        try:
            non_null = series.dropna()
            if len(non_null) == 0:
                return {
                    'range_type': 'datetime',
                    'min_value': None,
                    'max_value': None,
                    'data_range': None,
                }
                
            min_date = non_null.min()
            max_date = non_null.max()
            
            return {
                'range_type': 'datetime',
                'min_value': min_date,
                'max_value': max_date,
                'date_range_days': (max_date - min_date).days,
                'has_future_dates': max_date > pd.Timestamp.now(),
                'has_far_past_dates': min_date < pd.Timestamp.now() - pd.Timedelta(days=36500),  # ~100 years
                'data_range': f"{min_date.date()} to {max_date.date()}"
            }
        except Exception as e:
            return {
                'range_type': 'datetime',
                'min_value': None,
                'max_value': None,
                'data_range': f"Error: {str(e)}",
            }
    
    def _analyze_categorical_or_text(self, series):
        """Analyze categorical or text column"""
        try:
            non_null = series.dropna()
            if len(non_null) == 0:
                return {
                    'range_type': 'text',
                    'string_min_length': None,
                    'string_max_length': None,
                    'string_length_range': None,
                    'categories': None
                }
            
            # Convert to string safely
            non_null = non_null.astype(str)
            
            # Get string length stats
            str_lengths = non_null.str.len()
            str_min = str_lengths.min()
            str_max = str_lengths.max()
            str_avg = str_lengths.mean()
            
            result = {
                'string_min_length': int(str_min),
                'string_max_length': int(str_max),
                'string_avg_length': float(str_avg),
                'string_length_range': f"{str_min} to {str_max} chars",
            }
            
            # Determine if categorical
            unique_ratio = series.nunique() / len(series) if len(series) > 0 else 0
            
            if unique_ratio < 0.1 or (series.nunique() <= 20 and len(series) >= 20):
                result['range_type'] = 'categorical'
                top_values = series.value_counts().head(10).to_dict()
                result['categories'] = {str(k): int(v) for k, v in top_values.items()}
                # Get top 5 categories for display
                result['top_categories'] = list(top_values.keys())[:5]
            else:
                result['range_type'] = 'text'
                
                # Check for patterns
                has_emails = non_null.str.contains(r'@.*\.', regex=True).any()
                has_urls = non_null.str.contains(r'https?://', regex=True).any()
                has_numerics = non_null.str.contains(r'\d').any()
                
                result['has_email_pattern'] = has_emails
                result['has_url_pattern'] = has_urls
                result['has_numeric_values'] = has_numerics
            
            return result
        except Exception as e:
            return {
                'range_type': 'text',
                'string_min_length': None,
                'string_max_length': None,
                'string_length_range': f"Error: {str(e)}",
            }
    
    def _detect_column_patterns(self, series, column_name):
        """Detect common column patterns based on name and values"""
        patterns = {}
        col_lower = column_name.lower()
        
        # ID patterns
        if col_lower.endswith('_id') or col_lower.endswith('id') or col_lower == 'id':
            patterns['column_role'] = 'identifier'
            
        # Date patterns
        elif col_lower.endswith('_at') or col_lower.endswith('_date') or 'date' in col_lower or 'time' in col_lower:
            patterns['column_role'] = 'date'
            
        # Name/description patterns
        elif col_lower.endswith('_name') or col_lower == 'name' or col_lower.endswith('_desc'):
            patterns['column_role'] = 'description'
            
        # Status/flag patterns
        elif col_lower.endswith('_status') or col_lower.endswith('_flag') or col_lower == 'status':
            patterns['column_role'] = 'status'
            
        # Amount/value patterns
        elif col_lower.endswith('_amount') or col_lower.endswith('_value') or 'price' in col_lower or 'cost' in col_lower:
            patterns['column_role'] = 'measure'
            
        # SAP specific patterns
        elif col_lower in ('mandt', 'spras', 'werks', 'matnr', 'charg', 'vbeln', 'erdat', 'erzet'):
            patterns['column_role'] = 'sap_field'
            
        return patterns
    
    def _detect_primary_keys(self, df):
        """
        Detect primary key candidates
        
        Args:
            df: Pandas DataFrame
            
        Returns:
            List of primary key column names
        """
        primary_keys = []
        
        # Try common ID column patterns first
        id_columns = [col for col in df.columns if col.lower().endswith('_id') or col.lower() == 'id']
        
        for col in id_columns:
            if df[col].nunique() == len(df) and df[col].isna().sum() == 0:
                primary_keys.append(col)
                return primary_keys
        
        # If no obvious ID columns, check all columns
        for col in df.columns:
            if df[col].nunique() == len(df) and df[col].isna().sum() == 0:
                primary_keys.append(col)
                
                # Only add one primary key unless we find strong evidence for composite key
                if not any(c.lower().endswith('_id') for c in primary_keys):
                    break
        
        # Check for composite keys if no single primary key found
        if not primary_keys:
            # Try common composite key patterns
            common_pairs = [
                ('MANDT', 'MATNR'),  # SAP material with client
                ('MANDT', 'WERKS'),  # SAP plant with client
                ('MANDT', 'CHARG'),  # SAP batch with client
                ('year', 'month', 'day'),  # Date components
            ]
            
            for pair in common_pairs:
                if all(col in df.columns for col in pair):
                    if df.groupby(list(pair)).size().max() == 1:
                        primary_keys.extend(pair)
                        break
        
        return primary_keys
    
    def _find_relationships(self):
        """
        Find relationships between tables
        
        Returns:
            List of relationship dictionaries
        """
        relationships = []
        
        # Find tables with direct relations
        for table1_name, df1 in self.datasets.items():
            for table2_name, df2 in self.datasets.items():
                if table1_name != table2_name:
                    # Find matching column names - typical foreign key pattern
                    for col1 in df1.columns:
                        for col2 in df2.columns:
                            # Common patterns for related columns
                            if (col1 == col2 or 
                                (col1.endswith('_id') and col1 == f"{table2_name[:-1] if table2_name.endswith('s') else table2_name}_id") or
                                (col2.endswith('_id') and col2 == f"{table1_name[:-1] if table1_name.endswith('s') else table1_name}_id")):
                                
                                # Check for value overlap
                                overlap = self._calculate_overlap(df1[col1], df2[col2])
                                if overlap > 0.1:  # At least 10% overlap
                                    relationships.append({
                                        'from_table': table1_name,
                                        'from_column': col1,
                                        'to_table': table2_name,
                                        'to_column': col2,
                                        'match_percentage': round(overlap * 100, 1),
                                        'relationship_type': self._determine_relationship_type(df1, col1, df2, col2)
                                    })
        
        return relationships
    
    def _calculate_overlap(self, series1, series2):
        """Calculate overlap between two series"""
        # Handle numeric and non-numeric types differently
        try:
            if pd.api.types.is_numeric_dtype(series1) and pd.api.types.is_numeric_dtype(series2):
                # For numeric types
                values1 = set(series1.dropna().unique())
                values2 = set(series2.dropna().unique())
            else:
                # For other types, convert to string first
                values1 = set(series1.dropna().astype(str).unique())
                values2 = set(series2.dropna().astype(str).unique())
                
            # Calculate overlap
            overlap = values1.intersection(values2)
            return len(overlap) / len(values1) if len(values1) > 0 else 0
        except:
            return 0
    
    def _determine_relationship_type(self, df1, col1, df2, col2):
        """Determine likely relationship type (1:1, 1:N, N:M)"""
        # Check uniqueness in both directions
        unique1 = df1[col1].nunique() == len(df1.dropna(subset=[col1]))
        unique2 = df2[col2].nunique() == len(df2.dropna(subset=[col2]))
        
        if unique1 and unique2:
            return "1:1"
        elif unique1:
            return "1:N"
        elif unique2:
            return "N:1"
        else:
            return "N:M"
    
    def get_summary_stats(self):
        """
        Get summary statistics as a dictionary
        
        Returns:
            Dictionary with summary statistics
        """
        if self.summary is None:
            self.analyze()
            
        summary_stats = {
            'total_tables': len(self.datasets),
            'total_columns': self.summary['column_summaries']['column_name'].nunique(),
            'total_rows': self.summary['table_summaries']['row_count'].sum(),
            'total_relationships': len(self.summary['relationships']),
            'tables_with_primary_keys': self.summary['table_summaries']['primary_keys'].apply(lambda x: len(x) > 0).sum(),
            'orphan_tables': len(self.datasets) - len(set([r['from_table'] for r in self.summary['relationships']] + 
                                                       [r['to_table'] for r in self.summary['relationships']]))
        }
        
        return summary_stats
    
    def print_summary(self):
        """Print formatted database summary"""
        if self.summary is None:
            print("No analysis performed. Run analyze() first.")
            return
            
        try:
            from tabulate import tabulate
            tabulate_available = True
        except ImportError:
            tabulate_available = False
        
        # Print header
        print("\n" + "="*80)
        print("DATABASE STRUCTURE SUMMARY".center(80))
        print("="*80)
        
        # Table overview
        print("\nTABLE OVERVIEW:")
        print("-" * 80)
        table_data = []
        for _, row in self.summary['table_summaries'].iterrows():
            table_data.append([
                row['table_name'],
                f"{row['row_count']:,}",
                row['column_count'],
                row['primary_keys'] if row['primary_keys'] else 'None',
                f"{row['memory_usage_mb']:.2f} MB"
            ])
        
        headers = ['Table Name', 'Row Count', 'Column Count', 'Primary Keys', 'Memory']
        if tabulate_available:
            print(tabulate(table_data, headers=headers, tablefmt='grid'))
        else:
            # Simple alternative if tabulate is not available
            print(f"{headers[0]:<20} {headers[1]:<15} {headers[2]:<15} {headers[3]:<30} {headers[4]:<15}")
            print("-" * 95)
            for row in table_data:
                print(f"{row[0]:<20} {row[1]:<15} {row[2]:<15} {str(row[3]):<30} {row[4]:<15}")
        
        # Column details by table
        print("\nCOLUMN SPECIFICATIONS:")
        print("-" * 80)
        
        for table in self.summary['table_summaries']['table_name']:
            print(f"\n{table.upper()}:")
            cols_df = self.summary['column_summaries'][self.summary['column_summaries']['table_name'] == table]
            
            col_data = []
            for _, col in cols_df.iterrows():
                # Determine range info
                if col.get('range_type') == 'numeric':
                    range_info = col['data_range']
                elif col.get('range_type') == 'datetime':
                    range_info = col['data_range']
                elif col.get('range_type') == 'categorical':
                    if 'top_categories' in col and col['top_categories']:
                        range_info = f"Categories: {', '.join(map(str, col['top_categories'][:3]))}"
                        if len(col['top_categories']) > 3:
                            range_info += f"... ({len(col['top_categories'])} total)"
                    else:
                        range_info = f"Categories: {col['unique_count']} unique values"
                else:
                    range_info = col['string_length_range']
                
                # Key type
                key_type = 'PK' if col['is_primary_key'] else ('Unique' if col['is_unique'] else '')
                
                col_data.append([
                    col['column_name'],
                    col['data_type'],
                    range_info,
                    f"{col['null_count']} ({col['null_percentage']}%)",
                    key_type
                ])
            
            headers = ['Column Name', 'Data Type', 'Range/Categories', 'Null Count', 'Key Type']
            if tabulate_available:
                print(tabulate(col_data, headers=headers, tablefmt='grid'))
            else:
                print(f"{headers[0]:<20} {headers[1]:<15} {headers[2]:<30} {headers[3]:<20} {headers[4]:<10}")
                print("-" * 95)
                for row in col_data:
                    print(f"{row[0]:<20} {row[1]:<15} {str(row[2])[:30]:<30} {row[3]:<20} {row[4]:<10}")
        
        # Relationships
        print("\nTABLE RELATIONSHIPS:")
        print("-" * 80)
        
        if self.summary['relationships']:
            rel_data = []
            for rel in self.summary['relationships']:
                rel_data.append([
                    f"{rel['from_table']}.{rel['from_column']}",
                    "→",
                    f"{rel['to_table']}.{rel['to_column']}",
                    f"{rel['match_percentage']}%",
                    rel['relationship_type']
                ])
            
            headers = ['Source', '', 'Target', 'Match %', 'Type']
            if tabulate_available:
                print(tabulate(rel_data, headers=headers, tablefmt='grid'))
            else:
                print(f"{headers[0]:<30} {headers[1]:<5} {headers[2]:<30} {headers[3]:<10} {headers[4]:<10}")
                print("-" * 85)
                for row in rel_data:
                    print(f"{row[0]:<30} {row[1]:<5} {row[2]:<30} {row[3]:<10} {row[4]:<10}")
        else:
            print("No relationships detected")
        
        print("\n" + "="*80)
    
    def export_to_json(self, filepath):
        """
        Export summary to JSON
        
        Args:
            filepath: JSON file path
        """
        if self.summary is None:
            print("No analysis performed. Run analyze() first.")
            return
        
        import json
        
        export_data = {
            'table_summaries': self.summary['table_summaries'].to_dict(orient='records'),
            'column_summaries': self.summary['column_summaries'].to_dict(orient='records'),
            'relationships': self.summary['relationships'],
            'summary_stats': self.get_summary_stats()
        }
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
            
        print(f"Database analysis exported to {filepath}")
    
    def export_to_excel(self, filepath):
        """
        Export summary to Excel
        
        Args:
            filepath: Excel file path
        """
        if self.summary is None:
            print("No analysis performed. Run analyze() first.")
            return
        
        with pd.ExcelWriter(filepath) as writer:
            self.summary['table_summaries'].to_excel(writer, sheet_name='Tables', index=False)
            self.summary['column_summaries'].to_excel(writer, sheet_name='Columns', index=False)
            
            # Convert relationships to DataFrame
            rel_df = pd.DataFrame(self.summary['relationships'])
            if not rel_df.empty:
                rel_df.to_excel(writer, sheet_name='Relationships', index=False)
                
            # Summary stats
            summary_stats = pd.DataFrame([self.get_summary_stats()])
            summary_stats.to_excel(writer, sheet_name='Summary', index=False)
            
        print(f"Database analysis exported to {filepath}")

    def get_suggestions(self):
        """
        Get data quality improvement suggestions
        
        Returns:
            List of suggestions
        """
        if self.summary is None:
            self.analyze()
            
        suggestions = []
        
        # Table-level suggestions
        for _, table in self.summary['table_summaries'].iterrows():
            table_name = table['table_name']
            
            # Check for missing primary keys
            if len(table['primary_keys']) == 0:
                suggestions.append({
                    'object_type': 'table', 
                    'object_name': table_name,
                    'issue': 'missing_primary_key',
                    'description': f"Table '{table_name}' lacks a primary key which may lead to duplicate records.",
                    'suggestion': f"Consider adding a unique identifier column to '{table_name}'."
                })
                
            # Check for tables with too many columns
            if table['column_count'] > 50:
                suggestions.append({
                    'object_type': 'table', 
                    'object_name': table_name,
                    'issue': 'too_many_columns',
                    'description': f"Table '{table_name}' has {table['column_count']} columns which may indicate poor normalization.",
                    'suggestion': f"Consider normalizing '{table_name}' into multiple related tables."
                })
                
            # Check for very small tables
            if table['row_count'] < 5 and table['row_count'] > 0:
                suggestions.append({
                    'object_type': 'table', 
                    'object_name': table_name,
                    'issue': 'sparse_table',
                    'description': f"Table '{table_name}' has only {table['row_count']} rows, which may indicate reference data.",
                    'suggestion': f"Verify this is not an incomplete dataset or consider consolidating reference data."
                })
                
        # Column-level suggestions
        for _, column in self.summary['column_summaries'].iterrows():
            table_name = column['table_name']
            column_name = column['column_name']
            
            # High null percentage
            if column['null_percentage'] > 30:
                suggestions.append({
                    'object_type': 'column', 
                    'object_name': f"{table_name}.{column_name}",
                    'issue': 'high_null_percentage',
                    'description': f"Column '{column_name}' in table '{table_name}' has {column['null_percentage']}% null values.",
                    'suggestion': "Consider if this column is needed or add data quality rules to enforce non-null values."
                })
                
            # Low cardinality - potential for enumeration
            if column['range_type'] == 'text' and 1 < column['unique_count'] <= 10 and column['unique_count'] / column['null_count'] < 0.5 if column['null_count'] > 0 else True:
                suggestions.append({
                    'object_type': 'column', 
                    'object_name': f"{table_name}.{column_name}",
                    'issue': 'low_cardinality',
                    'description': f"Column '{column_name}' has only {column['unique_count']} unique values but is not defined as categorical.",
                    'suggestion': f"Consider using an enumeration or reference table for '{column_name}'."
                })
                
            # Numeric columns with outliers
            if column['range_type'] == 'numeric' and 'outlier_percentage' in column and column['outlier_percentage'] > 5:
                suggestions.append({
                    'object_type': 'column', 
                    'object_name': f"{table_name}.{column_name}",
                    'issue': 'outliers',
                    'description': f"Column '{column_name}' has {column['outlier_percentage']}% outlier values.",
                    'suggestion': f"Validate outliers in '{column_name}' and consider adding range validation rules."
                })
                
            # Datetime columns with future dates
            if column['range_type'] == 'datetime' and 'has_future_dates' in column and column['has_future_dates']:
                suggestions.append({
                    'object_type': 'column', 
                    'object_name': f"{table_name}.{column_name}",
                    'issue': 'future_dates',
                    'description': f"Column '{column_name}' contains future dates, which may indicate data entry errors.",
                    'suggestion': f"Add validation rules to ensure '{column_name}' does not accept future dates if inappropriate."
                })
                
        # Relationship suggestions
        tables_with_relationships = set()
        for rel in self.summary['relationships']:
            tables_with_relationships.add(rel['from_table'])
            tables_with_relationships.add(rel['to_table'])
            
        # Tables without relationships (isolated)
        for table_name in self.datasets.keys():
            if table_name not in tables_with_relationships:
                suggestions.append({
                    'object_type': 'table', 
                    'object_name': table_name,
                    'issue': 'isolated_table',
                    'description': f"Table '{table_name}' has no detected relationships with other tables.",
                    'suggestion': f"Verify if '{table_name}' should be related to other tables in the database."
                })
                
        return suggestions