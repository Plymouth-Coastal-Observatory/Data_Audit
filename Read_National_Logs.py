import pandas as pd
import os
import sys

def read_national_logs(national_audit_log:list, output_csv="Audit_CSV.csv"):

    all_read_logs_dfs = []
    for path in national_audit_log:

        qc_files_number = sum(1 for _ in open(path)) - 1  # Count rows excluding header
        print(f"📄 Input audit file: {national_audit_log}")
        print(f"🔢 Rows in file: {qc_files_number}")

        audit_df = pd.read_csv(path, delimiter=",")

        # Filter for topo only
        audit_df = audit_df[audit_df['Type'] == 'topographic']
        audit_df['Files'] = audit_df['Files'].str.split(';')
        audit_df['Original_Index'] = audit_df.index
        audit_df = audit_df.explode('Files').reset_index(drop=True)
        audit_df = audit_df[audit_df['Files'] != '']
        all_read_logs_dfs.append(audit_df)


    concat_result = pd.concat(all_read_logs_dfs)
    concat_result.to_csv(output_csv, index=False)
    print(f"✅ Done. Audit CSV saved to: {output_csv}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_audit_csv.py <path_to_national_audit_log> [output_csv]")
        sys.exit(1)

    audit_file = sys.argv[1]
    output_csv = sys.argv[2] if len(sys.argv) > 2 else "Audit_CSV.csv"

    read_national_logs(audit_file, output_csv)
