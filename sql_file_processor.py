"""
SQL File Processor
Converts SQL files (CREATE TABLE, INSERT, SELECT statements) to CSV format
so they can be processed by domain detectors using the same logic.
Enhanced to handle multiple tables, foreign keys, schema-only files, and relationship extraction.
"""
import re
import pandas as pd
import os
from sqlalchemy import text, inspect
from database import engine
import tempfile
from typing import Dict, List, Tuple, Optional


class SQLFileProcessor:
    """Process SQL files and convert them to CSV format for domain detection"""
    
    def __init__(self):
        self.temp_files = []  # Track temp files for cleanup
        self.extracted_relationships = []  # Store foreign key relationships from SQL
        self.extracted_tables = {}  # Store table schemas with constraints
        self.extracted_primary_keys = {}  # Store primary keys per table
        self.extracted_foreign_keys = []  # Store foreign key relationships
    
    def parse_sql_file(self, sql_file_path: str) -> str:
        """
        Parse SQL file and convert to CSV format.
        Returns path to temporary CSV file that can be used by domain detectors.
        
        Supports:
        - CREATE TABLE + INSERT statements
        - SELECT statements+
        - Multiple tables in one file
        - Schema-only files (CREATE TABLE without data)
        - Foreign key dependencies
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
            
            # Parse all CREATE TABLE statements to extract schema
            create_statements = []
            insert_statements = []
            select_statements = []
            table_schemas = {}  # Store schema info for each table
            
            # Reset extracted data for new file
            self.extracted_relationships = []
            self.extracted_tables = {}
            self.extracted_primary_keys = {}
            self.extracted_foreign_keys = []
            
            # Categorize statements
            for statement in statements:
                statement_upper = statement.upper().strip()
                
                if statement_upper.startswith('CREATE TABLE'):
                    create_statements.append(statement)
                    # Extract table name, columns, PKs, FKs from CREATE TABLE
                    table_info = self._parse_create_table(statement)
                    if table_info:
                        table_schemas[table_info['name']] = table_info
                        self.extracted_tables[table_info['name']] = table_info
                
                elif statement_upper.startswith('INSERT'):
                    insert_statements.append(statement)
                
                elif statement_upper.startswith('SELECT'):
                    select_statements.append(statement)
            
            # If we have SELECT statements, parse columns from them (avoid DB execution)
            if select_statements:
                # First, try to parse column names from SELECT statement
                parsed_columns = self._parse_select_columns(select_statements[0])
                
                # Also parse relationships from JOIN clauses
                select_relationships = self._parse_select_relationships(select_statements[0])
                if select_relationships:
                    # Store extracted relationships
                    self.extracted_foreign_keys.extend(select_relationships)
                    # Build relationship explanations
                    for rel in select_relationships:
                        self.extracted_relationships.append(rel)
                
                if parsed_columns:
                    # Create schema-based DataFrame from parsed columns
                    df = pd.DataFrame(columns=parsed_columns)
                    df.loc[0] = [''] * len(parsed_columns)  # Add one empty row
                    temp_csv_path = self._create_temp_csv(df, sql_file_path)
                    return temp_csv_path
                else:
                    # Fallback: Try to execute SELECT if parsing failed (e.g., SELECT *)
                    try:
                        df = self._execute_select(select_statements[0])
                        if df is not None and not df.empty:
                            temp_csv_path = self._create_temp_csv(df, sql_file_path)
                            return temp_csv_path
                    except Exception as e:
                        print(f"Warning: Could not execute SELECT statement: {e}")
                        # Continue to CREATE TABLE processing if SELECT fails
            
            # Try to execute CREATE TABLE and INSERT statements
            # Handle foreign key dependencies by temporarily disabling constraints
            df = None
            all_tables_data = []
            
            try:
                # Execute CREATE TABLE statements (handle foreign keys)
                self._execute_create_tables(create_statements)
                
                # Execute INSERT statements
                if insert_statements:
                    self._execute_inserts(insert_statements)
                
                # Try to get data from all tables
                for table_name in table_schemas.keys():
                    try:
                        table_df = self._execute_select(f"SELECT * FROM {table_name}")
                        if table_df is not None and not table_df.empty:
                            all_tables_data.append((table_name, table_df))
                    except Exception as e:
                        print(f"Warning: Could not query table {table_name}: {e}")
                        # Create empty DataFrame with schema columns
                        schema = table_schemas[table_name]
                        columns = schema.get('columns', [])
                        if columns:
                            table_df = pd.DataFrame(columns=columns)
                            all_tables_data.append((table_name, table_df))
                
                # Combine all tables into one DataFrame (if multiple tables)
                if all_tables_data:
                    if len(all_tables_data) == 1:
                        # Single table - use it directly
                        df = all_tables_data[0][1]
                    else:
                        # Multiple tables - combine all data with all columns
                        # Strategy: Create a union of all columns and concatenate rows
                        all_columns = set()
                        for table_name, table_df in all_tables_data:
                            all_columns.update(table_df.columns)
                        
                        # Create list of DataFrames with all columns
                        combined_dfs = []
                        for table_name, table_df in all_tables_data:
                            # Add missing columns to each DataFrame
                            for col in all_columns:
                                if col not in table_df.columns:
                                    table_df[col] = None
                            # Reorder columns consistently
                            table_df = table_df[list(all_columns)]
                            combined_dfs.append(table_df)
                        
                        # Concatenate vertically (stack rows)
                        df = pd.concat(combined_dfs, axis=0, ignore_index=True, sort=False)
                        # Fill NaN with empty string for missing columns
                        df = df.fillna('')
                
            except Exception as e:
                print(f"Warning: Database execution failed: {e}")
                # Fallback: Create CSV from schema information only
                if table_schemas:
                    df = self._create_df_from_schema(table_schemas)
            
            # If still no data, try to create from schema
            if df is None or df.empty:
                if table_schemas:
                    df = self._create_df_from_schema(table_schemas)
                else:
                    raise ValueError("No data or schema could be extracted from SQL file")
            
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
    
    def _parse_create_table(self, create_statement: str) -> Optional[Dict]:
        """Parse CREATE TABLE statement to extract table name, columns, PKs, and FKs"""
        try:
            # Extract table name
            table_match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)', 
                                   create_statement, re.IGNORECASE)
            if not table_match:
                return None
            
            table_name = table_match.group(1).strip()
            
            # Extract column definitions (between parentheses)
            col_match = re.search(r'\((.*)\)', create_statement, re.DOTALL | re.IGNORECASE)
            if not col_match:
                return {"name": table_name, "columns": [], "primary_key": None, "foreign_keys": []}
            
            column_defs = col_match.group(1)
            
            # Parse columns (split by comma, but handle nested parentheses for constraints)
            columns = []
            primary_key = None
            foreign_keys = []
            current_col = ""
            paren_depth = 0
            
            for char in column_defs:
                if char == '(':
                    paren_depth += 1
                    current_col += char
                elif char == ')':
                    paren_depth -= 1
                    current_col += char
                elif char == ',' and paren_depth == 0:
                    # End of column definition
                    col_def = current_col.strip()
                    if col_def:
                        result = self._parse_column_def(col_def, table_name)
                        if result:
                            if result.get('type') == 'column':
                                columns.append(result['name'])
                                if result.get('is_primary_key'):
                                    primary_key = result['name']
                            elif result.get('type') == 'foreign_key':
                                foreign_keys.append(result)
                            elif result.get('type') == 'primary_key_constraint':
                                primary_key = result.get('column')
                    current_col = ""
                else:
                    current_col += char
            
            # Handle last column
            if current_col.strip():
                col_def = current_col.strip()
                result = self._parse_column_def(col_def, table_name)
                if result:
                    if result.get('type') == 'column':
                        columns.append(result['name'])
                        if result.get('is_primary_key'):
                            primary_key = result['name']
                    elif result.get('type') == 'foreign_key':
                        foreign_keys.append(result)
                    elif result.get('type') == 'primary_key_constraint':
                        primary_key = result.get('column')
            
            # Store primary key for this table
            if primary_key:
                self.extracted_primary_keys[table_name] = primary_key
            
            # Store foreign keys
            for fk in foreign_keys:
                self.extracted_foreign_keys.append({
                    'child_table': table_name,
                    'child_column': fk.get('column'),
                    'parent_table': fk.get('references_table'),
                    'parent_column': fk.get('references_column'),
                    'relationship_type': 'FOREIGN_KEY'
                })
            
            return {
                "name": table_name, 
                "columns": columns, 
                "primary_key": primary_key, 
                "foreign_keys": foreign_keys
            }
        except Exception as e:
            print(f"Warning: Could not parse CREATE TABLE: {e}")
            return None
    
    def _parse_column_def(self, col_def: str, table_name: str) -> Optional[Dict]:
        """Parse a single column definition to extract column info, PKs, FKs"""
        col_def_upper = col_def.upper().strip()
        
        # Check if this is a FOREIGN KEY constraint
        if col_def_upper.startswith('FOREIGN KEY'):
            # Pattern: FOREIGN KEY (column) REFERENCES table(column)
            fk_match = re.search(
                r'FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+([^\s(]+)\s*\(([^)]+)\)',
                col_def, re.IGNORECASE
            )
            if fk_match:
                return {
                    'type': 'foreign_key',
                    'column': fk_match.group(1).strip(),
                    'references_table': fk_match.group(2).strip(),
                    'references_column': fk_match.group(3).strip()
                }
            return None
        
        # Check if this is a PRIMARY KEY constraint
        if col_def_upper.startswith('PRIMARY KEY'):
            pk_match = re.search(r'PRIMARY\s+KEY\s*\(([^)]+)\)', col_def, re.IGNORECASE)
            if pk_match:
                return {
                    'type': 'primary_key_constraint',
                    'column': pk_match.group(1).strip()
                }
            return None
        
        # Check if this is a CONSTRAINT (skip)
        if col_def_upper.startswith('CONSTRAINT'):
            return None
        
        # Regular column definition
        col_name_match = re.match(r'^\s*([^\s(]+)', col_def)
        if col_name_match:
            col_name = col_name_match.group(1).strip()
            is_primary_key = 'PRIMARY KEY' in col_def_upper
            return {
                'type': 'column',
                'name': col_name,
                'is_primary_key': is_primary_key
            }
        
        return None
    
    def get_extracted_relationships(self) -> List[Dict]:
        """Return the extracted foreign key relationships from SQL"""
        return self.extracted_foreign_keys
    
    def get_extracted_primary_keys(self) -> Dict[str, str]:
        """Return the extracted primary keys per table"""
        return self.extracted_primary_keys
    
    def get_extracted_foreign_keys(self) -> List[Dict]:
        """Return the extracted foreign keys from SQL"""
        return self.extracted_foreign_keys
    
    def get_sql_schema_info(self) -> Dict:
        """Return complete schema info extracted from SQL"""
        return {
            'tables': self.extracted_tables,
            'primary_keys': self.extracted_primary_keys,
            'foreign_keys': self.extracted_foreign_keys,
            'relationships': self._build_relationship_explanations()
        }
    
    def _build_relationship_explanations(self) -> List[Dict]:
        """Build human-readable relationship explanations from foreign keys and JOINs"""
        relationships = []
        for fk in self.extracted_foreign_keys:
            child_table = fk.get('child_table', '')
            child_col = fk.get('child_column', '')
            parent_table = fk.get('parent_table', '')
            parent_col = fk.get('parent_column', '')
            rel_type = fk.get('relationship_type', 'FOREIGN_KEY')
            
            # Determine explanation based on relationship type
            if rel_type == 'JOIN':
                type_label = 'JOIN'
                explanation = (f"The '{child_col}' column in '{child_table}' is joined with '{parent_col}' in '{parent_table}'. "
                              f"This JOIN relationship links records between these tables for combined queries.")
                business_rule = f"Queries joining {child_table} and {parent_table} use {child_col} = {parent_col} as the connection point."
            else:
                type_label = 'FOREIGN_KEY'
                explanation = (f"The '{child_col}' column in '{child_table}' references '{parent_col}' in '{parent_table}'. "
                              f"This indicates that each record in '{child_table}' is linked to a record in '{parent_table}'.")
                business_rule = f"Every {child_table} must have a valid {parent_table} reference through {child_col}."
            
            relationships.append({
                'relationship': f"{child_table}.{child_col} -> {parent_table}.{parent_col}",
                'child_table': child_table,
                'child_column': child_col,
                'parent_table': parent_table,
                'parent_column': parent_col,
                'type': type_label,
                'explanation': explanation,
                'business_rule': business_rule
            })
        
        return relationships
    
    def _parse_select_columns(self, select_statement: str) -> List[str]:
        """
        Parse SELECT statement to extract column names without executing against DB.
        This is safer for domain detection as it doesn't require tables to exist.
        
        Handles:
        - Simple columns: SELECT customer_id, first_name
        - Table aliases: SELECT c.customer_id, c.first_name
        - AS aliases: SELECT c.first_name AS fname
        - Returns empty list for SELECT * (needs DB execution)
        """
        try:
            # Remove comments and normalize whitespace
            clean_stmt = self._remove_comments(select_statement)
            clean_stmt = ' '.join(clean_stmt.split())
            
            # Extract the SELECT clause (between SELECT and FROM)
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM\s+', clean_stmt, re.IGNORECASE | re.DOTALL)
            if not select_match:
                return []
            
            select_clause = select_match.group(1).strip()
            
            # Check for SELECT * (return empty to trigger DB execution fallback)
            if select_clause.strip() == '*' or re.match(r'^\w+\.\*$', select_clause.strip()):
                return []
            
            # Split by commas (handling nested parentheses for functions)
            columns = []
            current_col = ""
            paren_depth = 0
            
            for char in select_clause:
                if char == '(':
                    paren_depth += 1
                    current_col += char
                elif char == ')':
                    paren_depth -= 1
                    current_col += char
                elif char == ',' and paren_depth == 0:
                    if current_col.strip():
                        columns.append(current_col.strip())
                    current_col = ""
                else:
                    current_col += char
            
            # Don't forget the last column
            if current_col.strip():
                columns.append(current_col.strip())
            
            # Extract actual column names from each column expression
            parsed_columns = []
            for col_expr in columns:
                col_name = self._extract_column_name(col_expr)
                if col_name:
                    parsed_columns.append(col_name)
            
            return parsed_columns
            
        except Exception as e:
            print(f"Warning: Could not parse SELECT columns: {e}")
            return []
    
    def _extract_column_name(self, col_expr: str) -> Optional[str]:
        """
        Extract the column name from a column expression.
        
        Examples:
        - 'customer_id' -> 'customer_id'
        - 'c.customer_id' -> 'customer_id'
        - 'c.first_name AS fname' -> 'first_name'
        - 'COUNT(*)' -> None (skip aggregates without alias)
        - 'COUNT(*) AS total' -> 'total'
        """
        col_expr = col_expr.strip()
        
        # Check for AS alias: use the alias name
        as_match = re.search(r'\s+AS\s+(\w+)\s*$', col_expr, re.IGNORECASE)
        if as_match:
            return as_match.group(1)
        
        # Check for table.column pattern: extract column name
        table_col_match = re.match(r'^(\w+)\.(\w+)\s*$', col_expr)
        if table_col_match:
            return table_col_match.group(2)
        
        # Check for simple column name
        simple_match = re.match(r'^(\w+)\s*$', col_expr)
        if simple_match:
            return simple_match.group(1)
        
        # For functions/expressions without AS, try to extract a meaningful name
        # Skip if it looks like an aggregate function without alias
        if '(' in col_expr:
            return None
        
        return None
    
    def _parse_select_relationships(self, select_statement: str) -> List[Dict]:
        """
        Parse SELECT statement to extract table relationships from JOIN clauses.
        
        Input: SELECT ... FROM table1 t1 JOIN table2 t2 ON t1.col = t2.col
        Output: [{'child_table': 'table2', 'child_column': 'col', 
                  'parent_table': 'table1', 'parent_column': 'col',
                  'relationship_type': 'JOIN'}]
        """
        relationships = []
        
        try:
            # Remove comments and normalize whitespace
            clean_stmt = self._remove_comments(select_statement)
            clean_stmt = ' '.join(clean_stmt.split())
            
            # Parse table aliases from FROM and JOIN clauses
            table_aliases = self._parse_table_aliases(clean_stmt)
            
            # Find all JOIN ... ON patterns
            # Matches: JOIN table alias ON alias1.col1 = alias2.col2
            join_pattern = r'JOIN\s+(\w+)(?:\s+(\w+))?\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)'
            join_matches = re.findall(join_pattern, clean_stmt, re.IGNORECASE)
            
            for match in join_matches:
                joined_table = match[0]  # The table being joined
                joined_alias = match[1] if match[1] else joined_table
                left_alias = match[2]
                left_col = match[3]
                right_alias = match[4]
                right_col = match[5]
                
                # Resolve aliases to actual table names
                left_table = table_aliases.get(left_alias.lower(), left_alias)
                right_table = table_aliases.get(right_alias.lower(), right_alias)
                
                # Determine parent/child (the joined table is typically the child)
                # The table in FROM clause is typically the parent
                if right_table.lower() == joined_table.lower() or right_alias.lower() == joined_alias.lower():
                    parent_table = left_table
                    parent_col = left_col
                    child_table = right_table
                    child_col = right_col
                else:
                    parent_table = right_table
                    parent_col = right_col
                    child_table = left_table
                    child_col = left_col
                
                relationship = {
                    'child_table': child_table,
                    'child_column': child_col,
                    'parent_table': parent_table,
                    'parent_column': parent_col,
                    'relationship_type': 'JOIN'
                }
                relationships.append(relationship)
                
                # Also store in extracted_tables if not already there
                for tbl in [parent_table, child_table]:
                    if tbl not in self.extracted_tables:
                        self.extracted_tables[tbl] = {
                            'name': tbl,
                            'columns': [],
                            'primary_key': None,
                            'foreign_keys': []
                        }
            
            return relationships
            
        except Exception as e:
            print(f"Warning: Could not parse SELECT relationships: {e}")
            return []
    
    def _parse_table_aliases(self, select_statement: str) -> Dict[str, str]:
        """
        Parse FROM and JOIN clauses to extract table name to alias mappings.
        
        Input: SELECT ... FROM customers c JOIN accounts a ON ...
        Output: {'c': 'customers', 'a': 'accounts', 'customers': 'customers', 'accounts': 'accounts'}
        """
        aliases = {}
        
        try:
            # Find FROM clause table: FROM table_name alias
            from_pattern = r'FROM\s+(\w+)(?:\s+(\w+))?'
            from_match = re.search(from_pattern, select_statement, re.IGNORECASE)
            if from_match:
                table_name = from_match.group(1)
                alias = from_match.group(2) if from_match.group(2) else table_name
                aliases[alias.lower()] = table_name
                aliases[table_name.lower()] = table_name
            
            # Find JOIN clause tables: JOIN table_name alias
            join_pattern = r'JOIN\s+(\w+)(?:\s+(\w+))?\s+ON'
            join_matches = re.findall(join_pattern, select_statement, re.IGNORECASE)
            for match in join_matches:
                table_name = match[0]
                alias = match[1] if match[1] else table_name
                aliases[alias.lower()] = table_name
                aliases[table_name.lower()] = table_name
            
        except Exception as e:
            print(f"Warning: Could not parse table aliases: {e}")
        
        return aliases
    
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
    
    def _execute_create_tables(self, create_statements: List[str]):
        """Execute CREATE TABLE statements, handling foreign key dependencies"""
        # First pass: Create tables without foreign key constraints
        for stmt in create_statements:
            # Extract table name and drop it first to ensure fresh schema
            try:
                table_name = self._extract_table_name(stmt)
                if table_name:
                    self._execute_drop_table(table_name)
            except Exception as e:
                print(f"Warning: Could not drop table explicitly: {e}")

            # Remove foreign key constraints temporarily
            stmt_modified = re.sub(r',\s*FOREIGN\s+KEY\s+\([^)]+\)\s+REFERENCES\s+[^,)]+', '', stmt, flags=re.IGNORECASE)
            stmt_modified = re.sub(r'FOREIGN\s+KEY\s+\([^)]+\)\s+REFERENCES\s+[^,)]+', '', stmt_modified, flags=re.IGNORECASE)
            try:
                self._execute_statement(stmt_modified)
            except Exception as e:
                # If still fails, try original statement
                try:
                    self._execute_statement(stmt)
                except Exception as e2:
                    print(f"Warning: Could not create table: {e2}")
    
    def _execute_drop_table(self, table_name: str):
        """Drop table if exists to ensure clean schema"""
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
        except Exception as e:
            print(f"Warning: Could not drop table {table_name}: {e}")
    
    def _create_df_from_schema(self, table_schemas: Dict) -> pd.DataFrame:
        """Create DataFrame from schema information when no data is available"""
        # Collect all unique columns from all tables
        all_columns_set = set()
        for table_name, schema in table_schemas.items():
            columns = schema.get('columns', [])
            all_columns_set.update(columns)
        
        if all_columns_set:
            # Create empty DataFrame with all columns (for schema-based prediction)
            # Add one empty row so DataFrame is not completely empty
            df = pd.DataFrame(columns=list(all_columns_set))
            # Add one row with empty values so the DataFrame structure is preserved
            df.loc[0] = [''] * len(all_columns_set)
            return df
        else:
            # Fallback: Create empty DataFrame with at least one column
            return pd.DataFrame(columns=['column_name'])
    
    def _execute_statement(self, statement: str):
        """Execute a DDL statement (CREATE TABLE)"""
        try:
            with engine.begin() as conn:  # begin() handles transaction automatically
                conn.execute(text(statement))
        except Exception as e:
            # If table already exists or other expected errors, continue
            error_str = str(e).lower()
            if ("already exists" in error_str or "duplicate" in error_str or 
                "relation" in error_str or "table" in error_str and "exists" in error_str):
                # Table already exists, that's okay
                pass
            else:
                # For foreign key errors, also continue (we'll handle it differently)
                if "foreign key" in error_str or "constraint" in error_str:
                    print(f"Warning: Foreign key constraint issue (continuing): {e}")
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
                        # Handle INSERT INTO table VALUES (row1), (row2), ... format
                        # Some databases require explicit column names, so try both
                        conn.execute(text(statement))
                    except Exception as e:
                        error_str = str(e).lower()
                        # Skip duplicate key errors, foreign key errors (parent table might not exist yet), but log others
                        if ("duplicate" in error_str or "unique" in error_str or 
                            "foreign key" in error_str or "constraint" in error_str or
                            "references" in error_str):
                            print(f"Warning: Insert constraint issue (skipping): {e}")
                            # Try to continue with next insert
                            continue
                        else:
                            # For other errors, log but continue
                            print(f"Warning: Insert error (continuing): {e}")
        except Exception as e:
            # Log but don't fail - data might already be inserted or constraints prevent it
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
