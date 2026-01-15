"""
Test script to verify banking domain analysis with pie chart functionality
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from bank import BankingDomainDetector

def test_banking_analysis():
    print("Testing Banking Domain Analysis with Pie Chart Generation...")
    
    # Initialize the detector
    detector = BankingDomainDetector()
    
    # Test with the existing bank.csv file
    csv_path = "bank.csv"
    
    if not os.path.exists(csv_path):
        print(f"CSV file {csv_path} not found. Creating a sample file...")
        # Create a sample CSV file for testing
        sample_data = """account_number,customer_name,ifsc_code,transaction_date,amount,balance
100001,Ramesh Kumar,SBIN0000456,2024-01-01,15000,25000
100002,Suresh Raj,SBIN0000456,2024-01-02,8200,16800
100003,Anitha Devi,HDFC0001234,2024-01-03,12500,32500"""
        
        with open(csv_path, "w") as f:
            f.write(sample_data)
    
    # Run the prediction
    result = detector.predict(csv_path)
    
    print("\nAnalysis Results:")
    print(f"Domain: {result['domain']}")
    print(f"Confidence Percentage: {result['confidence_percentage']}%")
    print(f"Decision: {result['decision']}")
    print(f"Total Columns: {result['total_columns']}")
    print(f"Matched Keywords: {result['matched_keywords']}")
    print(f"Matched Columns: {result['matched_columns']}")
    print(f"Chart Data Length: {len(result['chart_data']) if result.get('chart_data') else 0} characters")
    
    # Check if chart data exists
    if 'chart_data' in result and result['chart_data']:
        print("✅ Pie chart generation successful!")
        print("✅ Banking domain analysis with chart visualization is working!")
        
        # Save the chart to a file to verify it's valid
        import base64
        from PIL import Image
        from io import BytesIO
        
        # Decode the base64 image
        img_data = base64.b64decode(result['chart_data'])
        
        # Try to open the image to verify it's valid
        try:
            img = Image.open(BytesIO(img_data))
            print(f"✅ Chart image is valid: {img.format} format, size {img.size}")
            
            # Save the chart for inspection
            with open("test_banking_chart.png", "wb") as f:
                f.write(img_data)
            print("✅ Chart saved as test_banking_chart.png")
            
        except Exception as e:
            print(f"❌ Error opening chart image: {e}")
    else:
        print("❌ Chart data not found in result")
    
    return result

if __name__ == "__main__":
    test_banking_analysis()