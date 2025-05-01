import os
import csv
import multiprocessing
from tqdm import tqdm
from tkinter import Tk, filedialog

# Exclude this directory and anything inside it
EXCLUDE_DIR = r"X:\Data\Survey_Topo\Phase4\DeliveredData"

def find_holding_dirs(start_path, exclude_path=None):
    holding_dirs = []
    for root, dirs, _ in os.walk(start_path):
        # Skip if current path is inside excluded path
        if exclude_path and os.path.commonpath([os.path.abspath(root), os.path.abspath(exclude_path)]) == os.path.abspath(exclude_path):
            continue

        for d in dirs:
            if d.endswith("Batch"):
                full_path = os.path.join(root, d)
                holding_dirs.append(full_path)
    return holding_dirs

def worker(args):
    start_path, exclude_path = args
    return find_holding_dirs(start_path, exclude_path)

def get_all_root_dirs(base_path):
    """Get all top-level subdirectories to parallelize the search."""
    return [os.path.join(base_path, d) for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]

def choose_directory():
    Tk().withdraw()  # Hide main Tk window
    return filedialog.askdirectory(title="Select Base Directory")

def main():
    base_path = choose_directory()
    if not base_path:
        print("No directory selected. Exiting.")
        return

    sub_dirs = get_all_root_dirs(base_path)
    print(f"Scanning {len(sub_dirs)} subdirectories (excluding: {EXCLUDE_DIR})...")

    with multiprocessing.Pool() as pool:
        args_list = [(subdir, EXCLUDE_DIR) for subdir in sub_dirs]
        results = list(tqdm(pool.imap_unordered(worker, args_list), total=len(sub_dirs), desc="Searching"))

    all_matches = [match for sublist in results for match in sublist]

    output_csv = os.path.join(os.path.dirname(__file__), "All_Topo_Batch_Dirs.csv")
    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Batch Directory Path"])
        for path in all_matches:
            writer.writerow([path])

    print(f"\nDone. Found {len(all_matches)} directories ending in 'Batch'.")
    print(f"Results saved to: {output_csv}")

if __name__ == "__main__":
    multiprocessing.freeze_support()  # Required for Windows
    main()
