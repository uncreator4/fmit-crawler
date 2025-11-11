#!/usr/bin/env python3
"""
Convert JSON output to Excel file
Usage: python json_to_excel.py
"""
import json
import os
import glob
import pandas as pd

JSON_PATTERN = "data/fmit_data_*.json"
EXCEL_FILE = "data/fmit_data.xlsx"

def convert_json_to_excel():
    """Convert all numbered JSON files to a single Excel file."""
    # Find all JSON files matching the pattern
    json_files = sorted(glob.glob(JSON_PATTERN))
    
    if not json_files:
        print(f"❌ No JSON files found matching pattern: {JSON_PATTERN}")
        print("💡 Make sure the crawler has run and created data files.")
        return
    
    print(f"📖 Found {len(json_files)} JSON file(s)")
    
    all_data = []
    total_size = 0
    
    for json_file in json_files:
        try:
            print(f"   Reading {os.path.basename(json_file)}...")
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            if isinstance(data, list):
                all_data.extend(data)
                file_size = os.path.getsize(json_file) / 1024 / 1024
                total_size += file_size
                print(f"      ✅ {len(data)} records ({file_size:.2f} MB)")
            else:
                print(f"      ⚠️  Skipped (not an array)")
        except Exception as e:
            print(f"      ❌ Error reading {json_file}: {e}")
    
    if len(all_data) == 0:
        print("⚠️  No data found in JSON files.")
        return
    
    print(f"\n✅ Total records: {len(all_data)} (from {len(json_files)} file(s), {total_size:.2f} MB)")
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        # Ensure columns exist
        for col in ["url", "h1", "h2", "content"]:
            if col not in df.columns:
                df[col] = ""
        
        # Reorder columns
        df = df[["url", "h1", "h2", "content"]]
        
        # Convert to Excel
        print(f"\n💾 Converting to Excel: {EXCEL_FILE}")
        df.to_excel(EXCEL_FILE, index=False, engine='openpyxl')
        
        file_size = os.path.getsize(EXCEL_FILE) / 1024 / 1024
        print(f"✅ Successfully converted to Excel!")
        print(f"   File: {EXCEL_FILE}")
        print(f"   Records: {len(df)}")
        print(f"   Size: {file_size:.2f} MB")
        print(f"\n💡 You can now open {EXCEL_FILE} in Excel!")
        
    except Exception as e:
        print(f"❌ Error converting JSON to Excel: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    convert_json_to_excel()

