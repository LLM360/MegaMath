import argparse
import gzip
import io
import os
import random

def start_split(dump_file_name_paths, files_num, store_dir, shuffle, consecutive):
    """
    Splits the dump file listings into multiple files, either randomly or consecutively,
    depending on the flags provided.

    Args:
        dump_file_name_paths (list of str): Paths to the input dump file listings.
        files_num (int): Number of output files to generate.
        store_dir (str): Directory to store the output split files.
        shuffle (bool): Whether to shuffle the records before splitting.
        consecutive (bool): If True, store records consecutively across output files.
    """
    file_name_list = []

    # Load files and extend file name list accordingly
    for dump_file_name_path in dump_file_name_paths:
        if dump_file_name_path.endswith(".txt"):
            with open(dump_file_name_path) as records:
                file_name_list.extend(records)
        elif dump_file_name_path.endswith(".gz"):
            with gzip.open(dump_file_name_path, "rb") as stream:
                records = io.TextIOWrapper(stream, encoding="utf-8")
                file_name_list.extend(records)

    # Optionally shuffle the file name list
    if shuffle:
        random.shuffle(file_name_list)
    
    print(f"Total records: {len(file_name_list)}")

    # Ensure the storage directory exists
    if not os.path.exists(store_dir) and store_dir.endswith("/"):
        os.makedirs(store_dir, exist_ok=True)
    elif not os.path.exists(os.path.dirname(store_dir)):
        os.makedirs(os.path.dirname(store_dir), exist_ok=True)

    if consecutive:
        start_index = 0  # Initial index for consecutive file writing
        file_lines = len(file_name_list)
        base_lines_per_file = file_lines // files_num
        extra_lines = file_lines % files_num
    
    # Write records to output files
    for i in range(files_num):
        output_file = os.path.join(store_dir, f"Split_{i:03d}.txt") if store_dir.endswith("/") \
            else f"{store_dir}.Split_{i:03d}.txt"
        with open(output_file, "w") as f:
            if consecutive:
                # Determine the number of lines this file should get
                lines_for_this_file = base_lines_per_file + (1 if i < extra_lines else 0)
                end_index = start_index + lines_for_this_file

                # Write the designated slice of records to the file
                f.writelines(file_name_list[start_index:end_index])

                # Update the start index for the next file
                start_index = end_index
            else:
                # Distribute records skipping files_num indices for each record
                f.writelines(file_name_list[i::files_num])

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Split dump file listings into multiple files.")
    parser.add_argument('--file_path', nargs='+', help='Paths to the dump file listings')
    parser.add_argument('--files_num', type=int, default=99, help='Number of output files to generate')
    parser.add_argument('--store_dir', type=str, default='./split_files', help='Output directory for split files')
    parser.add_argument('--shuffle', type=bool, default=False, help='Shuffle the records before splitting')
    parser.add_argument('--consecutive', type=bool, default=False, help='Store consecutive listings in one file')

    args = parser.parse_args()
    start_split(args.file_path, args.files_num, args.store_dir, args.shuffle, args.consecutive)
