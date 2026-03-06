#!/usr/bin/env python3
"""
Script to clean up parquet historical data older than 7 days.
Useful for keeping disk usage low and dropping old history.
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import glob

def cleanup_old_data():
    days_to_keep = 7
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)
    
    base_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
    parquet_files = glob.glob(os.path.join(base_path, "*.parquet"))
    
    if not parquet_files:
        print("📂 No parquet files found to clean up.")
        return

    print(f"🧹 Scanning for data older than {cutoff_date.strftime('%Y-%m-%d')}...")

    for filepath in parquet_files:
        try:
            df = pd.read_parquet(filepath)
            original_len = len(df)
            
            # Find time column
            time_col = next((c for c in ['datetime', 'date', 'time'] if c in df.columns), None)
            
            if not time_col:
                print(f"⚠️ No time column found in {os.path.basename(filepath)}, skipping.")
                continue

            # Standardize to datetime type to filter
            if 'time' in df.columns and df['time'].dtype == 'O': 
                # Shoonya may prefix with "Ok "
                # So we parse carefully
                df['parsed_time'] = pd.to_datetime(
                    df['time'].astype(str).str.replace('Ok ', '', regex=False).str.strip(), 
                    format='%d-%m-%Y %H:%M:%S', errors='coerce'
                )
            else:
                df['parsed_time'] = pd.to_datetime(df[time_col], errors='coerce')
                
            # Filter rows
            mask = df['parsed_time'] >= cutoff_date
            filtered_df = df[mask].copy()
            filtered_df = filtered_df.drop(columns=['parsed_time'])
            
            new_len = len(filtered_df)
            removed = original_len - new_len
            
            if removed > 0:
                print(f"✂️  {os.path.basename(filepath)}: Removed {removed} old records.")
                filtered_df.to_parquet(filepath, engine='pyarrow', index=False)
            else:
                print(f"✅ {os.path.basename(filepath)}: No old records found.")
                
        except Exception as e:
            print(f"❌ Error processing {os.path.basename(filepath)}: {e}")

if __name__ == "__main__":
    print("=" * 60)
    print("🗑️  Shoonya Data Cleanup Script (7 Days config)")
    print("=" * 60)
    cleanup_old_data()
    print("✅ Cleanup finished.")
