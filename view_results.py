#!/usr/bin/env python3
"""
View the crawled data from parquet file
"""
import os
import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "data")
PARQUET_FILE = os.path.join(DATA_DIR, "fmit_data.parquet")
PAGE_CHECKPOINT = os.path.join(DATA_DIR, "page_checkpoint.json")

def view_results():
    """View the crawled data"""
    print("=" * 60)
    print("FMIT Crawler - Results Viewer")
    print("=" * 60)
    
    # Check checkpoint
    if os.path.exists(PAGE_CHECKPOINT):
        import json
        with open(PAGE_CHECKPOINT, "r", encoding="utf-8") as f:
            checkpoint = json.load(f)
            last_page = checkpoint.get("last_page", 0)
            print(f"\n📄 Last processed page: {last_page}")
            print(f"   Next run will start from page: {last_page + 1}")
    else:
        print("\n📄 No checkpoint found - crawler hasn't started yet")
    
    # Check parquet file
    if os.path.exists(PARQUET_FILE):
        print(f"\n📊 Data file: {PARQUET_FILE}")
        df = pd.read_parquet(PARQUET_FILE)
        
        print(f"\n✅ Total records: {len(df)}")
        print(f"   File size: {os.path.getsize(PARQUET_FILE) / 1024 / 1024:.2f} MB")
        
        # Show columns
        print(f"\n📋 Columns: {list(df.columns)}")
        
        # Show statistics
        print("\n📈 Statistics:")
        print(f"   URLs with h1: {df['h1'].notna().sum()}")
        print(f"   URLs with h2: {df['h2'].notna().sum()}")
        print(f"   URLs with content: {df['content'].notna().sum()}")
        print(f"   URLs with any content: {(df['h1'].notna() | df['h2'].notna() | df['content'].notna()).sum()}")
        
        # Show sample records
        print("\n" + "=" * 60)
        print("Sample Records (first 5):")
        print("=" * 60)
        for idx, row in df.head(5).iterrows():
            print(f"\n[{idx + 1}] URL: {row['url']}")
            if row['h1']:
                print(f"    H1: {row['h1'][:80]}...")
            if row['h2']:
                print(f"    H2: {row['h2'][:80]}...")
            if row['content']:
                content_preview = row['content'][:150].replace('\n', ' ')
                print(f"    Content: {content_preview}...")
        
        # Show recent records
        if len(df) > 5:
            print("\n" + "=" * 60)
            print("Recent Records (last 3):")
            print("=" * 60)
            for idx, row in df.tail(3).iterrows():
                print(f"\n[{len(df) - 2 + idx}] URL: {row['url']}")
                if row['h1']:
                    print(f"    H1: {row['h1'][:80]}...")
        
        # Option to export to CSV
        print("\n" + "=" * 60)
        print("💡 Tip: To export to CSV, run:")
        print(f"   python -c \"import pandas as pd; pd.read_parquet('{PARQUET_FILE}').to_csv('fmit_data.csv', index=False)\"")
        print("=" * 60)
        
    else:
        print(f"\n❌ No data file found at: {PARQUET_FILE}")
        print("   The crawler hasn't saved any data yet.")
        print("   Run the workflow or wait for the next scheduled run.")
    
    print()

if __name__ == "__main__":
    view_results()

