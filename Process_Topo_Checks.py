import pandas as pd
import re

AUDIT_CSV = "Audit_CSV.csv"  # your input audit list
FILE_LIST_CSV = "All_Topo_Batch_Files.csv"  # generated from previous script
OUTPUT_CSV = "SWCM_Topo_Audit_Results.csv"

def main():
    # Load the audit list and file list
    audit_df = pd.read_csv(AUDIT_CSV)
    files_df = pd.read_csv(FILE_LIST_CSV)

    # Make sure required columns exist
    if 'Files' not in audit_df.columns:
        print("Audit CSV must contain a 'Files' column.")
        return
    if 'Filename' not in files_df.columns:
        print("File list CSV must contain a 'Filename' column.")
        return

    # Perform lookup by filename- to filp the lookup order just change right- left.
    match_df = pd.merge(
        audit_df,
        files_df[['Filename', 'Full Path']],
        how='right',
        left_on='Files',
        right_on='Filename'
    )

    # Add a 'Found' column - Use for Left How
    #match_df['Found'] = match_df['Full Path'].notna().map({True: 'Yes', False: 'No'})

    # 'Found' means the file exists in the audit list -  Use for Right join
    match_df['Found'] = match_df['Files'].notna().map({True: 'Yes', False: 'No'})

    # filter file names for 2024 only:
    # Filter rows where 'Full Path' contains '2024'
    # Step 1: Filter for paths that contain '2024' followed by 4 digits
    match_df_filtered = match_df[match_df['Full Path'].str.contains(r'2024\d{4}', na=False)]

    # Step 2: Exclude rows where 'Full Path' contains 'Meta' (case-insensitive)
    match_df_filtered = match_df_filtered[
        ~match_df_filtered['Full Path'].str.contains(r'Meta', flags=re.IGNORECASE, na=False)]


    # Save result
    match_df_filtered.to_csv(OUTPUT_CSV, index=False)
    print(f"Done. Results written to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
