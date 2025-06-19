import os
import csv
import multiprocessing
from tqdm import tqdm
from tkinter import Tk, filedialog
import os
import csv
import multiprocessing
from tqdm import tqdm
from tkinter import Tk, filedialog

# Exclude multiple directories and anything inside them
EXCLUDE_DIRS = [
    r"X:\Data\Survey_Topo\Phase4\DeliveredData",
    r"X:\Data\Survey_Topo\Phase3\DeliveredData",  # Example additional exclusion path
    # Another exclusion path (can be modified)
]

def find_holding_dirs(start_path, exclude_paths=None):
    holding_dirs = []
    for root, dirs, _ in os.walk(start_path):
        # Skip if current path is inside any of the excluded paths
        if exclude_paths:
            for exclude_path in exclude_paths:
                if os.path.commonpath([os.path.abspath(root), os.path.abspath(exclude_path)]) == os.path.abspath(exclude_path):
                    continue

        for d in dirs:
            if d.endswith("Batch"):
                full_path = os.path.join(root, d)
                holding_dirs.append(full_path)
    return holding_dirs

def worker(args):
    start_path, exclude_paths = args
    return find_holding_dirs(start_path, exclude_paths)

def get_all_root_dirs(base_path):
    """Get all top-level subdirectories to parallelize the search."""
    return [os.path.join(base_path, d) for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]

def choose_directories():
    Tk().withdraw()  # Hide main Tk window
    directories = filedialog.askdirectory(title="Select Base Directory")  # This returns a single directory, change it for multiple
    if not directories:
        return []

    # Allow multiple directory selection by asking for directories in a loop
    directories = filedialog.askopenfilenames(
        title="Select Base Directories",
        filetypes=[("All files", "*.*")]
    )
    return list(directories)

def main():
    base_paths = choose_directories()
    if not base_paths:
        print("No directories selected. Exiting.")
        return

    all_matches = []

    for base_path in base_paths:
        sub_dirs = get_all_root_dirs(base_path)
        print(f"Scanning {len(sub_dirs)} subdirectories in: {base_path} (excluding: {EXCLUDE_DIRS})...")

        with multiprocessing.Pool() as pool:
            args_list = [(subdir, EXCLUDE_DIRS) for subdir in sub_dirs]
            results = list(tqdm(pool.imap_unordered(worker, args_list), total=len(sub_dirs), desc="Searching"))

        all_matches.extend([match for sublist in results for match in sublist])

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
