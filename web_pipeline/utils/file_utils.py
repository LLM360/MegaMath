import csv
import gzip
import json
import os
from dataclasses import asdict

import requests


def download_from_cc(
    object_key, bucket_name="commoncrawl", local_root_path="./crawl-data/"
):
    local_file = local_root_path + object_key
    local_file_path = os.path.dirname(local_file)
    print(f"Downloading {object_key} to {local_file}")
    if not os.path.exists(local_file_path):
        os.makedirs(local_file_path, exist_ok=True)
    try:
        # download the file from the url to local_file_path
        PREFIX = "https://data.commoncrawl.org/"
        url = PREFIX + object_key
        # show speed
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(local_file, "wb") as file:
            file.write(response.content)
        print(f"Successfully downloaded {object_key} to {local_file}")
        return local_file

        # assert False
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None


def write_to_jsonlgz(data, output_file):
    print(f"Writing {len(data)} documents into {output_file} ...")
    with gzip.open(output_file, "at", encoding="utf-8") as gz_file:
        gz_file.write("\n".join(json.dumps(item) for item in data) + "\n")


def delete_local_files(to_delete_files):
    for file_to_remove in to_delete_files:
        try:
            # Attempt to remove the file
            os.remove(file_to_remove)
            print(f"File '{file_to_remove}' has been successfully removed.")
        except FileNotFoundError:
            print(f"File '{file_to_remove}' not found.")
        except Exception as e:
            print(f"An error occurred: {str(e)}")


def make_dir(file_name):
    file_path = os.path.dirname(file_name)
    print(f"Making directory: {file_path}")
    if not os.path.exists(file_path):
        os.makedirs(file_path, exist_ok=True)


def remove_file(file_name):
    if os.path.isfile(file_name):
        os.remove(file_name)
        print(f"Remove halfly-processed file: {file_name}")


def write_stat(stat_file, statistics, input_file, FIELD_NAMES):
    make_dir(stat_file)
    print(f"Writing {str(input_file)} into {stat_file}")
    if not os.path.exists(stat_file):
        with open(stat_file, mode="a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=FIELD_NAMES)

            # Write the headers
            writer.writeheader()

            # Write the data as a dictionary
            writer.writerow(asdict(statistics))
    else:
        with open(stat_file, mode="a", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=FIELD_NAMES)

            # Write the data as a dictionary
            writer.writerow(asdict(statistics))
