#!/usr/bin/env python3
"""
Merge JSON array files by combining arrays and removing duplicates.
Used to resolve Git merge conflicts in JSON data files.
"""
import json
import sys
import os

def merge_json_files(ours_path: str, theirs_path: str, base_path: str, output_path: str) -> None:
    """Merge two JSON array files, removing duplicates by URL."""
    # Read all three versions
    ours_data = []
    theirs_data = []
    base_data = []
    
    if os.path.exists(ours_path):
        with open(ours_path, 'r', encoding='utf-8') as f:
            ours_data = json.load(f)
            if not isinstance(ours_data, list):
                ours_data = []
    
    if os.path.exists(theirs_path):
        with open(theirs_path, 'r', encoding='utf-8') as f:
            theirs_data = json.load(f)
            if not isinstance(theirs_data, list):
                theirs_data = []
    
    if os.path.exists(base_path):
        with open(base_path, 'r', encoding='utf-8') as f:
            base_data = json.load(f)
            if not isinstance(base_data, list):
                base_data = []
    
    # Combine all records
    all_records = ours_data + theirs_data
    
    # Remove duplicates by URL (keep first occurrence)
    seen_urls = set()
    merged_data = []
    for record in all_records:
        url = record.get("url", "")
        if url and url not in seen_urls:
            merged_data.append(record)
            seen_urls.add(url)
    
    # Write merged result
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Merged {len(ours_data)} + {len(theirs_data)} records = {len(merged_data)} unique records")

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: merge_json.py <ours> <theirs> <base> <output>")
        sys.exit(1)
    
    merge_json_files(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

