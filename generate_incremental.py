import pandas as pd
import numpy as np

# Load data
csv_df = pd.read_csv("netflix_titles.csv")
excel_df = pd.read_excel("1000_netflix_titles.xlsx")

# Drop duplicate show_id within each source
csv_df = csv_df.drop_duplicates(subset='show_id')
excel_df = excel_df.drop_duplicates(subset='show_id')

# Sample 50 rows from each
csv_sample = csv_df.sample(n=min(50, len(csv_df)), random_state=42)
excel_sample = excel_df.sample(n=min(50, len(excel_df)), random_state=42)

# Ensure no overlapping show_id
excel_sample = excel_sample[~excel_sample['show_id'].isin(csv_sample['show_id'])]

# Add 'state' column
csv_sample['state'] = 0
excel_sample['state'] = np.random.choice([0, 1, 2], size=len(excel_sample))

# Combine and trim to 100 rows
combined_df = pd.concat([csv_sample, excel_sample]).head(100)

# Save to Excel
combined_df.to_excel("incremental_load.xlsx", index=False)

print("✅ Finished creating incremental_load.xlsx")
