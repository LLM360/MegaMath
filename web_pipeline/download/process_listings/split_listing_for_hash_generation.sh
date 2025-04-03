#!/bin/bash

# Initialize an array to store the names of new dumps
declare -a ALL_DUMPS

# Read each line from the dumplist.txt file and append to the ALL_DUMPS array
# This file contains a list of new dump directories
while IFS= read -r line; do
    ALL_DUMPS+=("$line")
done < "commoncrawlList/dumplist.txt"

for dump_date in "${ALL_DUMPS[@]}"; do
    all_file_paths=()
    file_path="commoncrawlList/$dump_date/warc.paths.gz"
    all_file_paths+=("$file_path")

    # Execute a Python script to process the listed WARC file paths
    # --files_num: Specifies the number of output files to generate
    # --store_dir: Defines the directory where the split listings will be stored
    # --file_path: Passes the array of WARC file paths to the Python script
    # --shuffle: Enables shuffling of the file paths before processing 
    echo $file_path
    python3 split_listing.py \
        --files_num 20 \
        --store_dir ../../listings/hash_generation/run-1/$dump_date \
        --file_path "${all_file_paths[@]}" \
        --shuffle True     
done