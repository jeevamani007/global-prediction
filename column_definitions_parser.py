"""
Column Definitions Parser
Parses the .md file to extract banking column definitions
"""

import re
import os
from typing import Dict, List, Tuple
from column_definitions_loader import COLUMN_DEFINITIONS


class ColumnDefinitionsParser:
    """Parse .md file to extract column definitions"""
    
    def __init__(self, md_file_path: str = None):
        """
        Initialize parser with path to .md file
        
        Args:
            md_file_path: Path to the .md file containing column definitions (defaults to "../.md")
        """
        if md_file_path is None:
            # Default to parent directory .md file
            self.md_file_path = ".md"
        else:
            self.md_file_path = md_file_path
        # Load from pre-defined definitions first (from column_definitions_loader)
        self.column_definitions = COLUMN_DEFINITIONS.copy()
        # Try to parse .md file to add/override any definitions
        self._parse_md_file()
    
    def _parse_md_file(self):
        """Parse the .md file and extract column definitions"""
        try:
            # Try multiple possible paths
            possible_paths = [
                self.md_file_path,  # Current directory
                os.path.join('..', self.md_file_path),  # Parent directory
                os.path.join(os.path.dirname(__file__), '..', self.md_file_path),  # Relative to this file
                os.path.join(os.path.dirname(os.path.dirname(__file__)), self.md_file_path),  # Two levels up
                os.path.join(os.getcwd(), self.md_file_path),  # Current working directory
            ]
            
            content = None
            for path in possible_paths:
                try:
                    if os.path.exists(path):
                        with open(path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        print(f"Successfully loaded .md file from: {os.path.abspath(path)}")
                        break
                except Exception as e:
                    continue
            
            if content is None:
                print(f"Warning: Could not find .md file. Tried paths: {possible_paths}")
                # Try to list current directory to help debug
                try:
                    current_dir = os.path.dirname(__file__)
                    print(f"Current directory: {current_dir}")
                    if os.path.exists(current_dir):
                        files = [f for f in os.listdir(current_dir) if f.endswith('.md')]
                        print(f"Found .md files in current directory: {files}")
                except:
                    pass
                return
            
            # Extract column definitions using regex
            # Pattern: column_name - description (handles various formats)
            pattern = r'^([a-z_]+(?:[a-z0-9_]+)?)\s*-\s*(.+)$'
            
            lines = content.split('\n')
            current_section = "General"
            
            for line in lines:
                line = line.strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Check for section headers
                # Pattern: "Customer & Account Columns (1-20)" or "Location & Identity Columns (21-35)"
                if 'Columns' in line and ('(' in line or '&' in line):
                    # Extract section name before "Columns"
                    section_match = re.match(r'^(.+?)\s+Columns', line, re.IGNORECASE)
                    if section_match:
                        current_section = section_match.group(1).strip()
                        # Clean up section name
                        current_section = current_section.replace('&', 'and').strip()
                    else:
                        # Fallback: use line content before "Columns"
                        current_section = line.split('Columns')[0].strip()
                    
                    if not current_section:
                        current_section = "General"
                    continue
                
                # Match column definitions: "column_name - description"
                # Example: "customer_id - Unique identifier for each customer..."
                match = re.match(pattern, line, re.IGNORECASE)
                if match:
                    column_name = match.group(1).strip().lower()
                    description = match.group(2).strip()
                    
                    # Store with section information
                    self.column_definitions[column_name] = {
                        "description": description,
                        "section": current_section,
                        "full_description": description
                    }
        
        except Exception as e:
            print(f"Error parsing .md file: {str(e)}")
            import traceback
            traceback.print_exc()
    
    def get_column_definition(self, column_name: str) -> Dict:
        """
        Get definition for a column name (supports variations)
        
        Args:
            column_name: Column name to look up (can be normalized)
            
        Returns:
            Dictionary with column definition or None
        """
        # Normalize column name
        normalized = self._normalize_column_name(column_name)
        
        # Direct match
        if normalized in self.column_definitions:
            return self.column_definitions[normalized]
        
        # Try partial matches
        for key, value in self.column_definitions.items():
            if key in normalized or normalized in key:
                return value
        
        # Try removing common prefixes/suffixes
        base_name = normalized.replace('_id', '').replace('_number', '').replace('_code', '')
        for key, value in self.column_definitions.items():
            base_key = key.replace('_id', '').replace('_number', '').replace('_code', '')
            if base_name == base_key:
                return value
        
        return None
    
    def _normalize_column_name(self, column_name: str) -> str:
        """Normalize column name for matching"""
        return column_name.lower().strip().replace(' ', '_').replace('-', '_')
    
    def get_all_definitions(self) -> Dict:
        """Get all column definitions"""
        return self.column_definitions
    
    def get_section_definitions(self, section: str) -> Dict:
        """Get all definitions for a specific section"""
        return {
            k: v for k, v in self.column_definitions.items()
            if v.get("section", "").lower() == section.lower()
        }


# Global instance for easy access
_parser_instance = None


def get_column_parser() -> ColumnDefinitionsParser:
    """Get or create global parser instance"""
    global _parser_instance
    if _parser_instance is None:
        _parser_instance = ColumnDefinitionsParser()
    return _parser_instance


def get_column_definition(column_name: str) -> Dict:
    """Convenience function to get column definition"""
    parser = get_column_parser()
    return parser.get_column_definition(column_name)
