import os
import csv
import multiprocessing
from tqdm import tqdm

INPUT_CSV = "All_Topo_Batch_Dirs.csv"
OUTPUT_CSV = "All_Topo_Batch_Files.csv"

def list_files_in_dir(directory):
    files_info = []
    for root, _, files in os.walk(directory):
        for f in files:
            # all os tile ascii files are upper case
            if f.endswith(".asc"):
                # Split the filename into the name and the extension
                name, ext = os.path.splitext(f)

                # Split the name by the first underscore
                parts = name.split('_')

                # Convert only the first part to uppercase
                parts[0] = parts[0].upper()

                # Reassemble the name and extension
                f = "_".join(parts) + ext

            full_path = os.path.join(root, f)
            files_info.append((directory, f, full_path))
    return files_info

def worker(directory):
    return list_files_in_dir(directory)

def read_directories_from_csv(csv_path):
    directories = []
    with open(csv_path, newline='') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if row:
                directories.append(row[0])
    return directories

def main():
    if not os.path.exists(INPUT_CSV):
        print(f"Input CSV '{INPUT_CSV}' not found.")
        return

    directories = read_directories_from_csv(INPUT_CSV)
    print(f"Found {len(directories)} directories to scan for files.")

    with multiprocessing.Pool() as pool:
        results = list(tqdm(pool.imap_unordered(worker, directories), total=len(directories), desc="Scanning"))

    all_files = [file for sublist in results for file in sublist]

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Directory", "Filename", "Full Path"])
        for directory, filename, full_path in all_files:
            writer.writerow([directory, filename, full_path])

    print(f"\nDone. {len(all_files)} files found.")
    print(f"Results saved to: {OUTPUT_CSV}")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
