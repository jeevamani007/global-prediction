"""
File Converter Utility
Converts various file formats (Excel, SQL) to CSV format
so they can be processed by domain detectors.
"""
import pandas as pd
import os
from sql_file_processor import SQLFileProcessor


class FileConverter:
    """Convert various file formats to CSV for domain detection"""
    
    def __init__(self):
        self.sql_processor = SQLFileProcessor()
        self.temp_files = []
    
    def convert_to_csv(self, file_path: str) -> str:
        """
        Convert file to CSV format.
        Supports: .csv, .xlsx, .xls, .sql
        Returns path to CSV file (original or converted)
        """
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.csv':
            # Already CSV, return as-is
            return file_path
        
        elif file_ext in ['.xlsx', '.xls']:
            # Convert Excel to CSV
            return self._excel_to_csv(file_path)
        
        elif file_ext == '.sql':
            # Convert SQL to CSV
            return self.sql_processor.parse_sql_file(file_path)
        
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
    
    def _excel_to_csv(self, excel_path: str) -> str:
        """Convert Excel file to CSV"""
        try:
            # Read Excel file
            df = pd.read_excel(excel_path)
            
            # Create CSV path
            base_name = os.path.splitext(os.path.basename(excel_path))[0]
            temp_dir = "uploads"
            os.makedirs(temp_dir, exist_ok=True)
            
            csv_path = os.path.join(temp_dir, f"{base_name}_converted.csv")
            
            # Save as CSV
            df.to_csv(csv_path, index=False)
            self.temp_files.append(csv_path)
            
            return csv_path
        except Exception as e:
            raise ValueError(f"Error converting Excel to CSV: {str(e)}")
    
    def cleanup_temp_files(self):
        """Clean up temporary files"""
        # Clean up SQL processor temp files
        if self.sql_processor:
            self.sql_processor.cleanup_temp_files()
        
        # Clean up Excel temp files
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.remove(temp_file)
            except Exception as e:
                print(f"Warning: Could not delete temp file {temp_file}: {e}")
        self.temp_files.clear()
