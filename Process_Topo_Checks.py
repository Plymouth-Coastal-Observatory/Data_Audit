import pandas as pd

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

    # Perform lookup by filename
    match_df = pd.merge(
        audit_df,
        files_df[['Filename', 'Full Path']],
        how='left',
        left_on='Files',
        right_on='Filename'
    )

    # Add a 'Found' column
    match_df['Found'] = match_df['Full Path'].notna().map({True: 'Yes', False: 'No'})

    # Save result
    match_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Done. Results written to {OUTPUT_CSV}")

if __name__ == "__main__":
    main()
