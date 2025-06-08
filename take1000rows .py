import pandas as pd

# Load the Excel file
df = pd.read_csv("netflix_titles.csv")

# Randomly sample 1000 rows (without replacement)
sample_df = df.sample(n=1000, random_state=42)

# Save the sample to a new Excel file
sample_df.to_excel("1000_netflix_titles.xlsx", index=False)
