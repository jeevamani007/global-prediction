"""
SQL File Processor
Converts SQL files (CREATE TABLE, INSERT, SELECT statements) to CSV format
so they can be processed by domain detectors using the same logic.
"""
import re
import pandas as pd
import os
from sqlalchemy import text, inspect
from database import engine
import tempfile


class SQLFileProcessor:
    """Process SQL files and convert them to CSV format for domain detection"""
    
    def __init__(self):
        self.temp_files = []  # Track temp files for cleanup
    
    def parse_sql_file(self, sql_file_path: str) -> str:
        """
        Parse SQL file and convert to CSV format.
        Returns path to temporary CSV file that can be used by domain detectors.
        
        Supports:
        - CREATE TABLE + INSERT statements
        - SELECT statements
        - Multiple statements
        """
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            # Remove comments and clean up
            sql_content = self._remove_comments(sql_content)
            
            # Split by semicolons to get individual statements
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]
            
            if not statements:
                raise ValueError("No valid SQL statements found in file")
            
            # Process statements
            df = None
            table_name = None
            create_statements = []
            insert_statements = []
            select_statements = []
            
            # Categorize statements
            for statement in statements:
                statement_upper = statement.upper().strip()
                
                if statement_upper.startswith('CREATE TABLE'):
                    create_statements.append(statement)
                    if not table_name:
                        table_name = self._extract_table_name(statement)
                
                elif statement_upper.startswith('INSERT'):
                    insert_statements.append(statement)
                    if not table_name:
                        table_name = self._extract_table_name_from_insert(statement)
                
                elif statement_upper.startswith('SELECT'):
                    select_statements.append(statement)
            
            # Execute CREATE TABLE statements first
            for stmt in create_statements:
                self._execute_statement(stmt)
            
            # Execute INSERT statements in a transaction
            if insert_statements:
                self._execute_inserts(insert_statements)
            
            # Execute SELECT statements (use first one)
            if select_statements:
                df = self._execute_select(select_statements[0])
            
            # If we have CREATE/INSERT but no SELECT, query the table
            if df is None and table_name:
                df = self._execute_select(f"SELECT * FROM {table_name}")
            
            if df is None or df.empty:
                raise ValueError("No data could be extracted from SQL file")
            
            # Create temporary CSV file
            temp_csv_path = self._create_temp_csv(df, sql_file_path)
            return temp_csv_path
            
        except Exception as e:
            raise ValueError(f"Error processing SQL file: {str(e)}")
    
    def _remove_comments(self, sql_content: str) -> str:
        """Remove SQL comments from content"""
        # Remove single-line comments (--)
        lines = sql_content.split('\n')
        cleaned_lines = []
        for line in lines:
            # Remove everything after --
            if '--' in line:
                line = line[:line.index('--')]
            cleaned_lines.append(line)
        
        # Remove multi-line comments (/* */)
        content = '\n'.join(cleaned_lines)
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        
        return content
    
    def _extract_table_name(self, create_statement: str) -> str:
        """Extract table name from CREATE TABLE statement"""
        match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)', 
                         create_statement, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        raise ValueError("Could not extract table name from CREATE TABLE statement")
    
    def _extract_table_name_from_insert(self, insert_statement: str) -> str:
        """Extract table name from INSERT statement"""
        match = re.search(r'INSERT\s+INTO\s+([^\s(]+)', 
                         insert_statement, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        raise ValueError("Could not extract table name from INSERT statement")
    
    def _execute_statement(self, statement: str):
        """Execute a DDL statement (CREATE TABLE)"""
        try:
            with engine.begin() as conn:  # begin() handles transaction automatically
                conn.execute(text(statement))
        except Exception as e:
            # If table already exists or other expected errors, continue
            error_str = str(e).lower()
            if "already exists" in error_str or "duplicate" in error_str or "relation" in error_str:
                # Table already exists, that's okay
                pass
            else:
                # Re-raise unexpected errors
                raise
    
    def _execute_inserts(self, insert_statements: list):
        """Execute multiple INSERT statements in a single transaction"""
        try:
            with engine.begin() as conn:
                for statement in insert_statements:
                    try:
                        conn.execute(text(statement))
                    except Exception as e:
                        error_str = str(e).lower()
                        # Skip duplicate key errors, but raise others
                        if "duplicate" not in error_str and "unique" not in error_str:
                            raise
        except Exception as e:
            # Log but don't fail - data might already be inserted
            print(f"Warning executing INSERT statements: {e}")
    
    def _execute_select(self, select_statement: str) -> pd.DataFrame:
        """Execute SELECT statement and return DataFrame"""
        try:
            with engine.connect() as conn:
                result = conn.execute(text(select_statement))
                rows = result.fetchall()
                columns = result.keys()
                
                if not rows:
                    return pd.DataFrame()
                
                df = pd.DataFrame(rows, columns=columns)
                return df
        except Exception as e:
            raise ValueError(f"Error executing SELECT: {str(e)}")
    
    def _create_temp_csv(self, df: pd.DataFrame, original_file_path: str) -> str:
        """Create temporary CSV file from DataFrame"""
        # Create temp file with same base name as original
        base_name = os.path.splitext(os.path.basename(original_file_path))[0]
        temp_dir = "uploads"
        os.makedirs(temp_dir, exist_ok=True)
        
        temp_csv_path = os.path.join(temp_dir, f"{base_name}_converted.csv")
        
        # Save DataFrame to CSV
        df.to_csv(temp_csv_path, index=False)
        self.temp_files.append(temp_csv_path)
        
        return temp_csv_path
    
    def cleanup_temp_files(self):
        """Clean up temporary CSV files"""
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                print(f"Warning: Could not delete temp file {temp_file}: {e}")
        self.temp_files.clear()
