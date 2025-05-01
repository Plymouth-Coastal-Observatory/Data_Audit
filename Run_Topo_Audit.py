import subprocess
import sys

"""
Run_Topo_Audit.py

This script orchestrates a sequence of quality control (QC) data processing steps for topographic audit logs.

It runs several subordinate scripts in order:

1. Extract_All_Topo_Batch_Dirs.py  
   - Recursively scans a directory structure to find all folder paths that end in "Batch" (excluding specific folders).
   - Outputs results to a CSV file.

2. Extract_Batch_Files.py  
   - Reads the list of batch directories and extracts information about the files they contain.

3. Read_National_Logs.py  
   - Takes a national audit log CSV file and filters for 'topographic' entries.
   - Explodes multi-file entries into separate rows and outputs a cleaned-up CSV.

4. Process_Topo_Checks.py  
   - Cross-references extracted batch file data and audit logs to identify any missing or mismatched items.

Only `Read_National_Logs.py` and `Process_Topo_Checks.py` are active by default.
To run the full pipeline, uncomment the relevant lines in `main()`.

Usage:
    python Run_Topo_Audit.py

Make sure to update the path to the national audit log CSV in the `main()` function.


Limitations/Assumptions:
- The script assumes that the Python environment is set up correctly and that all required packages are installed.
- It only looks for files in directories that end with "Batch". 
- Incorrect naming or missing files in the national audit log may lead to incomplete results.
- The python environment is set manually in the script, see subprocess.run cmd.
- All asc files in the Audit logs are upper case, the scripts convert the found .asc files to upper case.


"""


def run_script(script_name, *args):
    try:
        subprocess.run(
            [r"C:\Users\darle\PycharmProjects\Data_Audit\.venv\Scripts\python.exe", script_name, *args],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_name}")
        print(f"Exit code: {e.returncode}")
        print(f"Command: {e.cmd}")
        sys.exit(1)

def main():
    # run_script("Extract_All_Topo_Batch_Dirs.py")
    #run_script("Extract_Batch_Files.py")
    run_script("Read_National_Logs.py", r"C:\Users\darle\Downloads\AuditLogs_2024\Southwest_AuditLogs_2024.csv")
    run_script("Process_Topo_Checks.py")

if __name__ == "__main__":
    main()