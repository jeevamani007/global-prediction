"""
Folder-Based Application Analyzer
Analyzes all CSV files within a folder as a single application unit.
Detects cross-file relationships, infers file roles, and maintains folder isolation.
"""

import os
import pandas as pd
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import re


class FolderBasedApplicationAnalyzer:
    """
    Analyzes a folder containing multiple CSV files as a single application.
    Discovers files, detects relationships, and infers file roles.
    """
    
    def __init__(self):
        """Initialize the folder analyzer."""
        # File role indicators based on column patterns and data characteristics
        self.role_indicators = {
            'Master Data': {
                'patterns': [
                    r'master|main|primary',
                    r'account.*master|customer.*master|product.*master',
                ],
                'characteristics': [
                    'has_primary_keys',
                    'referenced_by_others',
                    'fewer_rows_than_transaction'
                ]
            },
            'Transaction Data': {
                'patterns': [
                    r'transaction|trans|txn',
                    r'history|log|activity',
                    r'entries|movements|ledger',
                ],
                'characteristics': [
                    'has_foreign_keys',
                    'has_timestamps',
                    'high_row_count',
                    'references_master'
                ]
            },
            'Reference Data': {
                'patterns': [
                    r'reference|ref|lookup',
                    r'code|type|status|category',
                    r'branch|location|region',
                ],
                'characteristics': [
                    'small_row_count',
                    'referenced_by_others',
                    'has_code_description_pairs'
                ]
            },
            'Mapping Data': {
                'patterns': [
                    r'mapping|map|link',
                    r'relation|relationship',
                    r'bridge|junction',
                ],
                'characteristics': [
                    'has_multiple_foreign_keys',
                    'primarily_key_columns',
                    'creates_many_to_many'
                ]
            },
            'History Data': {
                'patterns': [
                    r'history|historical|archive',
                    r'old|previous|past',
                    r'audit|trail|log',
                ],
                'characteristics': [
                    'has_timestamps',
                    'has_version_or_date_columns',
                    'tracks_changes'
                ]
            },
        }
    
    def analyze_folder(self, folder_path: str) -> Dict[str, Any]:
        """
        Analyze all CSV files in a folder as a single application.
        
        Args:
            folder_path: Path to the folder containing CSV files
            
        Returns:
            {
                'folder_name': 'banking_app',
                'folder_path': '/path/to/folder',
                'csv_files': ['accounts.csv', 'transactions.csv', 'customers.csv'],
                'csv_files_data': {filename: DataFrame},
                'file_roles': {'accounts.csv': 'Master Data', ...},
                'schema_patterns': {...},
                'cross_file_relationships': [...],
                'total_files': 3,
                'total_rows': 15000,
                'total_columns': 45
            }
        """
        
        # Validate folder
        if not os.path.exists(folder_path):
            return {'error': f'Folder not found: {folder_path}'}
        
        if not os.path.isdir(folder_path):
            return {'error': f'Path is not a directory: {folder_path}'}
        
        # Discover CSV files
        csv_files = self.discover_csv_files(folder_path)
        
        if not csv_files:
            return {
                'folder_name': os.path.basename(folder_path),
                'folder_path': folder_path,
                'csv_files': [],
                'error': 'No CSV files found in folder'
            }
        
        # Load all CSV files
        csv_files_data = {}
        total_rows = 0
        total_columns = 0
        
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                filename = os.path.basename(csv_file)
                csv_files_data[filename] = df
                total_rows += len(df)
                total_columns += len(df.columns)
            except Exception as e:
                print(f"Warning: Could not read {csv_file}: {str(e)}")
                continue
        
        if not csv_files_data:
            return {
                'folder_name': os.path.basename(folder_path),
                'folder_path': folder_path,
                'csv_files': [os.path.basename(f) for f in csv_files],
                'error': 'Could not read any CSV files'
            }
        
        # Detect schema patterns
        schema_patterns = self.detect_schema_patterns(csv_files_data)
        
        # Detect cross-file relationships
        cross_file_relationships = self.detect_cross_file_relationships(csv_files_data)
        
        # Infer file roles
        file_roles = self.infer_file_roles(csv_files_data, cross_file_relationships)
        
        return {
            'folder_name': os.path.basename(folder_path),
            'folder_path': folder_path,
            'csv_files': list(csv_files_data.keys()),
            'csv_files_data': csv_files_data,
            'file_roles': file_roles,
            'schema_patterns': schema_patterns,
            'cross_file_relationships': cross_file_relationships,
            'total_files': len(csv_files_data),
            'total_rows': total_rows,
            'total_columns': total_columns,
            'unique_column_count': len(schema_patterns.get('all_unique_columns', []))
        }
    
    def discover_csv_files(self, folder_path: str, recursive: bool = False) -> List[str]:
        """
        Discover all CSV files in a folder.
        
        Args:
            folder_path: Path to search
            recursive: Whether to search subdirectories
            
        Returns:
            List of absolute paths to CSV files
        """
        csv_files = []
        
        if recursive:
            # Search recursively
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    if file.lower().endswith('.csv'):
                        csv_files.append(os.path.join(root, file))
        else:
            # Search only top level
            for file in os.listdir(folder_path):
                file_path = os.path.join(folder_path, file)
                if os.path.isfile(file_path) and file.lower().endswith('.csv'):
                    csv_files.append(file_path)
        
        return sorted(csv_files)
    
    def detect_schema_patterns(self, csv_files_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """
        Detect schema patterns across multiple CSV files.
        
        Returns:
            {
                'all_unique_columns': [...],
                'common_columns': [...],
                'column_frequency': {...},
                'data_types': {...},
                'potential_keys': {...}
            }
        """
        all_columns = []
        column_frequency = {}
        potential_keys = {}
        
        # Collect all columns and their frequencies
        for filename, df in csv_files_data.items():
            for col in df.columns:
                col_lower = col.lower()
                all_columns.append(col_lower)
                column_frequency[col_lower] = column_frequency.get(col_lower, 0) + 1
                
                # Detect potential primary keys (columns with "id", "number", "code" that are unique)
                if self._is_potential_key(col, df):
                    if filename not in potential_keys:
                        potential_keys[filename] = []
                    potential_keys[filename].append(col)
        
        # Find common columns (appearing in multiple files)
        common_columns = [col for col, freq in column_frequency.items() if freq > 1]
        
        # Get unique columns
        all_unique_columns = list(set(all_columns))
        
        return {
            'all_unique_columns': sorted(all_unique_columns),
            'common_columns': sorted(common_columns),
            'column_frequency': column_frequency,
            'potential_keys': potential_keys,
            'total_unique_columns': len(all_unique_columns)
        }
    
    def _is_potential_key(self, column_name: str, df: pd.DataFrame) -> bool:
        """Check if a column is potentially a primary or foreign key."""
        col_lower = column_name.lower()
        
        # Check for key-like naming patterns
        key_patterns = [
            r'_id$', r'^id$', r'_number$', r'_code$', r'_key$',
            r'account.*number', r'customer.*id', r'transaction.*id',
            r'loan.*id', r'payment.*id', r'card.*number'
        ]
        
        has_key_pattern = any(re.search(pattern, col_lower) for pattern in key_patterns)
        
        if not has_key_pattern:
            return False
        
        # Check if values are mostly unique (at least 80% unique)
        try:
            if len(df) == 0:
                return False
            uniqueness_ratio = df[column_name].nunique() / len(df)
            return uniqueness_ratio >= 0.8
        except:
            return False
    
    def detect_cross_file_relationships(self, csv_files_data: Dict[str, pd.DataFrame]) -> List[Dict[str, Any]]:
        """
        Detect relationships between CSV files based on common columns and data overlap.
        
        Returns:
            List of relationship dictionaries with details
        """
        relationships = []
        
        filenames = list(csv_files_data.keys())
        
        # Compare each pair of files
        for i, file1 in enumerate(filenames):
            for file2 in filenames[i+1:]:
                df1 = csv_files_data[file1]
                df2 = csv_files_data[file2]
                
                # Find common columns
                common_cols = set(df1.columns) & set(df2.columns)
                
                for col in common_cols:
                    # Check if this column creates a relationship
                    relationship = self._analyze_column_relationship(file1, df1, file2, df2, col)
                    
                    if relationship:
                        relationships.append(relationship)
        
        return relationships
    
    def _analyze_column_relationship(
        self, 
        file1: str, 
        df1: pd.DataFrame, 
        file2: str, 
        df2: pd.DataFrame,
        column: str
    ) -> Optional[Dict[str, Any]]:
        """Analyze if a common column creates a meaningful relationship between two files."""
        
        try:
            # Get unique values from both files
            values1 = set(df1[column].dropna().unique())
            values2 = set(df2[column].dropna().unique())
            
            if not values1 or not values2:
                return None
            
            # Calculate overlap
            overlap = values1 & values2
            overlap_percentage = len(overlap) / max(len(values1), len(values2)) * 100
            
            # Only consider it a relationship if there's significant overlap (>10%)
            if overlap_percentage < 10:
                return None
            
            # Determine relationship type
            relationship_type = self._determine_relationship_type(
                len(values1), len(values2), len(overlap), column
            )
            
            return {
                'file1': file1,
                'file2': file2,
                'column': column,
                'relationship_type': relationship_type,
                'overlap_count': len(overlap),
                'overlap_percentage': round(overlap_percentage, 1),
                'file1_unique_count': len(values1),
                'file2_unique_count': len(values2),
                'explanation': self._generate_relationship_explanation(
                    file1, file2, column, relationship_type, overlap_percentage
                )
            }
        except Exception as e:
            print(f"Warning: Could not analyze relationship for column {column}: {str(e)}")
            return None
    
    def _determine_relationship_type(
        self, 
        count1: int, 
        count2: int, 
        overlap: int,
        column: str
    ) -> str:
        """Determine the type of relationship between two files."""
        
        col_lower = column.lower()
        
        # Check if it's likely a foreign key reference
        if 'id' in col_lower or 'number' in col_lower or 'code' in col_lower:
            # One-to-many relationship (master-detail)
            if count1 < count2 * 0.8:
                return 'One-to-Many (Master-Detail)'
            elif count2 < count1 * 0.8:
                return 'Many-to-One (Detail-Master)'
            else:
                return 'Many-to-Many'
        
        # Similar counts suggest a one-to-one or shared reference
        if abs(count1 - count2) < max(count1, count2) * 0.2:
            return 'Shared Reference'
        
        return 'Related'
    
    def _generate_relationship_explanation(
        self, 
        file1: str, 
        file2: str, 
        column: str,
        rel_type: str,
        overlap_pct: float
    ) -> str:
        """Generate human-readable explanation of the relationship."""
        
        return (
            f"The '{column}' column connects {file1} and {file2} with {overlap_pct:.1f}% data overlap. "
            f"This suggests a {rel_type} relationship, where records from both files can be linked "
            f"using this common identifier."
        )
    
    def infer_file_roles(
        self, 
        csv_files_data: Dict[str, pd.DataFrame],
        relationships: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """
        Infer the role of each CSV file (Master, Transaction, Reference, Mapping, History).
        
        Returns:
            Dict of {filename: role}
        """
        file_roles = {}
        
        for filename, df in csv_files_data.items():
            role = self._determine_file_role(filename, df, csv_files_data, relationships)
            file_roles[filename] = role
        
        return file_roles
    
    def _determine_file_role(
        self,
        filename: str,
        df: pd.DataFrame,
        all_files: Dict[str, pd.DataFrame],
        relationships: List[Dict[str, Any]]
    ) -> str:
        """Determine the role of a single file."""
        
        role_scores = {role: 0 for role in self.role_indicators.keys()}
        
        # Check filename patterns
        filename_lower = filename.lower()
        for role, indicators in self.role_indicators.items():
            for pattern in indicators['patterns']:
                if re.search(pattern, filename_lower):
                    role_scores[role] += 2
        
        # Check data characteristics
        row_count = len(df)
        avg_row_count = sum(len(d) for d in all_files.values()) / len(all_files)
        
        # Transaction data typically has more rows
        if row_count > avg_row_count * 1.5:
            role_scores['Transaction Data'] += 2
            role_scores['History Data'] += 1
        
        # Master/Reference data typically has fewer rows
        if row_count < avg_row_count * 0.5:
            role_scores['Master Data'] += 1
            role_scores['Reference Data'] += 2
        
        # Check if file is referenced by others (master data characteristic)
        references_to_this = sum(
            1 for rel in relationships
            if rel['file2'] == filename and 'Many' in rel.get('relationship_type', '')
        )
        if references_to_this > 0:
            role_scores['Master Data'] += 3
            role_scores['Reference Data'] += 2
        
        # Check if file references others (transaction data characteristic)
        references_from_this = sum(
            1 for rel in relationships
            if rel['file1'] == filename and 'Many' in rel.get('relationship_type', '')
        )
        if references_from_this > 0:
            role_scores['Transaction Data'] += 3
        
        # Check for timestamp columns (transaction/history characteristic)
        timestamp_patterns = [r'date', r'time', r'timestamp', r'created', r'updated']
        has_timestamps = any(
            any(re.search(pattern, col.lower()) for pattern in timestamp_patterns)
            for col in df.columns
        )
        if has_timestamps:
            role_scores['Transaction Data'] += 2
            role_scores['History Data'] += 2
        
        # Return the role with highest score
        best_role = max(role_scores.items(), key=lambda x: x[1])
        
        # If no clear winner (score is 0 or tied), return 'Data File'
        if best_role[1] == 0:
            return 'Data File'
        
        return best_role[0]


def analyze_folder(folder_path: str) -> Dict[str, Any]:
    """
    Convenience function to analyze a folder.
    
    Args:
        folder_path: Path to folder containing CSV files
        
    Returns:
        Folder analysis result
    """
    analyzer = FolderBasedApplicationAnalyzer()
    return analyzer.analyze_folder(folder_path)
