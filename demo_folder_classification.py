"""
Quick demo script to show folder-based classification with visual output.
Analyzes test folders and generates a sample JSON response.
"""

import json
from folder_based_application_analyzer import FolderBasedApplicationAnalyzer
from application_type_detector import ApplicationTypeDetector

def run_demo():
    """Run a quick demo of the folder-based classification system."""
    
    print("\n" + "="*100)
    print(" " * 30 + "FOLDER-BASED APPLICATION CLASSIFICATION DEMO")
    print("="*100 + "\n")
    
    # Folders to analyze
    test_folders = [
        ("test_data/core_banking", "Core Banking Application"),
        ("test_data/loan_management", "Loan Management Application"),
        ("test_data/payments", "Payments Application")
    ]
    
    folder_analyzer = FolderBasedApplicationAnalyzer()
    app_type_detector = ApplicationTypeDetector()
    
    results = {}
    
    for folder_path, description in test_folders:
        print(f"\n{'─'*100}")
        print(f"🔍 Analyzing: {description}")
        print(f"📁 Path: {folder_path}")
        print(f"{'─'*100}\n")
        
        # Analyze folder
        analysis = folder_analyzer.analyze_folder(folder_path)
        
        if 'error' in analysis:
            print(f"❌ Error: {analysis['error']}\n")
            continue
        
        # Detect application type
        app_type = app_type_detector.detect_type(
            analysis['csv_files_data'],
            relationships=analysis['cross_file_relationships']
        )
        
        # Display results
        print(f"✨ Application Type: {app_type['application_type']}")
        print(f"📊 Confidence: {app_type['confidence']}% ({app_type['confidence_level']})")
        print(f"📄 CSV Files: {len(analysis['csv_files'])}")
        print(f"🔗 Relationships: {len(analysis['cross_file_relationships'])}")
        print(f"📈 Total Rows: {analysis['total_rows']:,}")
        
        print(f"\n📋 Files and Roles:")
        for file, role in analysis['file_roles'].items():
            print(f"   • {file:<30} → {role}")
        
        if analysis['cross_file_relationships']:
            print(f"\n🔗 File Relationships:")
            for rel in analysis['cross_file_relationships']:
                print(f"   • {rel['file1']} ↔ {rel['file2']}")
                print(f"     Column: {rel['column']} | {rel['relationship_type']} | {rel['overlap_percentage']}% overlap")
        
        print(f"\n🎯 Top Patterns Detected:")
        for pattern in app_type['patterns_detected'][:5]:
            print(f"   • {pattern}")
        
        # Store for JSON output
        results[folder_path.split('/')[-1]] = {
            'folder_name': folder_path.split('/')[-1],
            'application_type': app_type,
            'csv_files': analysis['csv_files'],
            'file_roles': analysis['file_roles'],
            'file_relationships': analysis['cross_file_relationships'],
            'total_files': analysis['total_files'],
            'total_rows': analysis['total_rows'],
            'total_columns': analysis['total_columns'],
            'unique_column_count': analysis['unique_column_count'],
            'status': 'success'
        }
    
    print(f"\n{'='*100}")
    print(" " * 40 + "DEMO COMPLETE!")
    print(f"{'='*100}\n")
    
    # Save sample response
    sample_response = {
        'message': 'Folders analyzed successfully',
        'multi_folder_mode': True,
        'total_folders': len(results),
        'folders': results
    }
    
    with open('sample_multi_folder_response.json', 'w') as f:
        json.dump(sample_response, f, indent=2)
    
    print("💾 Sample API response saved to: sample_multi_folder_response.json")
    print("🌐 To view in UI, copy the JSON content to sessionStorage:")
    print("   sessionStorage.setItem('multi_folder_analysis', '<paste_json_here>')")
    print(f"\n{'='*100}\n")


if __name__ == "__main__":
    run_demo()
