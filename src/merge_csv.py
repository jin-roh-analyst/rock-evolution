import pandas as pd
import glob

# Match all split CSV files
csv_files = glob.glob('data/enriched/artist_genres_raw*_mb.csv')

# Read and combine all
df_list = [pd.read_csv(f) for f in csv_files]
merged_df = pd.concat(df_list, ignore_index=True)

# Save to a single file
merged_df.to_csv('merged.csv', index=False)

print("✅ Done! Created merged.csv from split files.")
