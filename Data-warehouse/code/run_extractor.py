from DataExtractor import DataExtractor
import os

def main():
    # Sử dụng đường dẫn tuyệt đối
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(os.path.dirname(current_dir), 'data')
    
    print(f"Looking for data in: {data_dir}")
    extractor = DataExtractor(output_dir=data_dir)
    
    # Test web crawling
    try:
        print("\n1. Testing web crawling from PNJ...")
        json_file = extractor.extract_from_pnj()
        print(f"Data saved to: {json_file}")
    except Exception as e:
        print(f"Error extracting from PNJ website: {str(e)}")
    
    # Test CSV file extraction
    try:
        print("\n2. Testing CSV file extraction...")
        csv_file = os.path.join(data_dir, "gold_price.csv")
        
        if os.path.exists(csv_file):
            print(f"Found CSV file: {csv_file}")
            # Xuất ra cả JSON và CSV
            output_json = extractor.extract_from_csv(csv_file, output_format='json')
            output_csv = extractor.extract_from_csv(csv_file, output_format='csv')
            print(f"Data saved to JSON: {output_json}")
            print(f"Data saved to CSV: {output_csv}")
        else:
            print(f"CSV file not found at: {csv_file}")
    except Exception as e:
        print(f"Error extracting from CSV: {str(e)}")
    
    # Test Excel file extraction
    try:
        print("\n3. Testing Excel file extraction...")
        excel_file = os.path.join(data_dir, "gold_price.xlsx")
        if os.path.exists(excel_file):
            print(f"Found Excel file: {excel_file}")
            output_json = extractor.extract_from_excel(excel_file, output_format='json')
            output_csv = extractor.extract_from_excel(excel_file, output_format='csv')
            print(f"Data saved to JSON: {output_json}")
            print(f"Data saved to CSV: {output_csv}")
        else:
            print(f"Excel file not found at: {excel_file}")
    except Exception as e:
        print(f"Error extracting from Excel: {str(e)}")
    
    # Test JSON file extraction
    try:
        print("\n4. Testing JSON file extraction...")
        # Sử dụng file JSON đã được tạo từ web crawling
        if os.path.exists(json_file):
            print(f"Found JSON file: {json_file}")
            output_json = extractor.extract_from_json(json_file, output_format='json')
            output_csv = extractor.extract_from_json(json_file, output_format='csv')
            print(f"Data saved to JSON: {output_json}")
            print(f"Data saved to CSV: {output_csv}")
        else:
            print(f"JSON file not found")
    except Exception as e:
        print(f"Error extracting from JSON: {str(e)}")

if __name__ == "__main__":
    main()

