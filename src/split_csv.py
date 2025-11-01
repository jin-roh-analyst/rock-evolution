import pandas as pd

# Load your CSV
df = pd.read_csv('data/cleaned/unique_artists_levels.csv')

# Number of splits you want
num_splits = 4

# Calculate roughly how many rows per split
rows_per_split = len(df) // num_splits + 1

# Split and save
for i in range(num_splits):
    start = i * rows_per_split
    end = (i + 1) * rows_per_split
    df_subset = df.iloc[start:end]
    df_subset.to_csv(f'split_{i+1}.csv', index=False)

print("✅ Done! Created 4 split CSV files.")
