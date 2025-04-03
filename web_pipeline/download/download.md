# Web Data Processing 
This folder contains a data processing pipeline for web data sourced from Common Crawl.

## Overview
To minimize local storage usage, we store all intermediate data in an AWS S3 bucket and process it using SageMaker to avoid incurring high data transfer fees and long downloading time from S3.
Our pipeline processes raw WARC files from Common Crawl through several high-level stages: text extraction, quality filtering, deduplication, and re-organization. The detailed processing stages are listed in the following section.

## Pipeline Stages
1. [Text Extraction](#1-text-extraction)
- Input: WARC files downloaded from Common Crawl S3 bucket
- Output: `.jsonl.gz` files with texts extracted from WARC format

2. [Quality Filtering](#2-quality-filtering)
- Input: all documents extracted from WARC in .jsonl.gz format
- Output: remaining documents after language filtering, url filtering, line-level removal, and document-level filtering

3. [Local Exact Deduplication](#3-local-exact-deduplication)
- Input: documents remaining after Quality Filtering
- Output: locally deduplicated documents via Bloom Filter

4. [Hash Value Generation](#4-hash-generation)
- Input: documents after the local exact deduplication
- Output: hash values for the input documents

5. [Global Deduplication](#5-global-deduplication)
- Input: hash values for documents after the local exact deduplication
- Output: Doc ids to remove and duplicates meta information

6. [Duplicates Output Processing](#6-duplicates-output-processing)
- Input: pickle file (duplicate information) from deduplication
- Output: duplicates info merged by file ids

7. [Duplicates Removal (and meta information update)](#7-duplicates-removal)
- Input:  new documents and documents after the last round of deduplication
- Output: documents remaining after duplicates removal and updated by new duplicates meta information

8. [Data Re-Organizing](#8-re-organize-data)
- Input: documents from last step (Remove Duplicates) with the file structure as Common Crawl
- Output: documents re-organized based on the duplicates meta information

---


All scripts provided below are compatible with SageMaker Studio. 
Certain scripts can also be executed locally, provided they do not require access to the text data in S3 bucket. 
However, the scripts (`scripts/sagemaker_pipeline`) that directly process text data are specifically tailored for SageMaker nodes and must be run on SageMaker Studio.

*Please Note: It is assumed that all commands mentioned below are executed from the main directory of this folder.*

---
## Prerequisite 

### Downloading Necessary Files
Please download the language identification model, which is [lib.176.bin](https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin) in our case, and save it to `scripts/sagemaker_env/language_id_model/`.

### Building Docker Image  
Build a docker image under folder `scripts/sagemaker_env/` and push it to Amazon Elastic Container Registry, which requires that your AWS CLI is properly configured.

### Updateing Variables 
Update AWS-related variables (`BUCKET_NAME`, `LISTING_PATH`,`ECR_IMAGE_URI`) in `scripts/sagemaker_pipeline/s3_variables.py`:
- `BUCKET_NAME`: your AWS s3 bucket in which all intermediate results will be stored
- `LISTING_PATH`: the path to the folder storing listings under your AWS s3 bucket
- `ECR_IMAGE_URI`: the image uri that you got after pushing the docker image into Amazon Elastic Container Registry.


### Downloading CC File Listings
Retrieve the file listings for all the dumps from Common Crawl by executing the following commands:
```bash
cd scripts/process_listings
python download_cc_list.py
```
After executing the script, the dump file listings will be downloaded and saved in the directory `scripts/process_listings/commoncrawlList`. 
These listings, which include all the WARC file paths located in the Common Crawl S3 bucket, 
are essential for nearly all subsequent processing steps, ensuring each file is processed accurately. 
Specifically, the file listings will be divided into several splits, with each CPU node managing one or more splits. 
Within each node, individual processes will handle one Common Crawl file from their assigned splits at a time.

## 1. Text Extraction
### Generating listings
Text extraction is the most resource-intensive step in our process, requiring approximately 50000 - 80000 CPU core hours per dump. 
To manage this effectively, the Common Crawl (CC) listings can be divided based on the number of available CPU nodes and the acceptable duration for the task. 
For example, if you have access to 200 CPU nodes, each equipped with 72 cores, and you aim to extract data from 2 dumps, you could distribute the CC listings into 200 splits. 
This setting would approximately take between 7 to 11 hours to complete.

The splitting can be accomplished by executing the following commands:
```bash
cd scripts/process_listings
bash split_listing_for_text_extraction.sh
```
This script will create 200 splits and save them in the directory `listings/text_extraction/run-1/`. The designation `run-1` allows for multiple iterations, facilitating re-runs for any files not successfully processed in the initial attempt.

Prior to initiating the text extraction process on SageMaker, ensure to upload your listings to the S3 bucket using the path specified in `scripts/sagemaker_pipeline/s3_variables.py`, let's say, `s3://your-s3-bucket/path/to/listings/under/s3/bucket/text_extraction/run-1`, using the following command:
```bash
aws s3 sync listings/text_extraction/run-1 s3://your-s3-bucket/path/to/listings/under/s3/bucket/text_extraction/run-1
```
This setup ensures that all necessary data is in place for the subsequent extraction tasks to proceed smoothly on SageMaker.

### Executing Text Extraction
Next, you could run the text extraction on AWS cpu nodes through SageMaker. 
Please note that the maximum number of instances per processing job is capped at 20 and this limit is not adjustable. 
Therefore, you need to manually divide the 200 listings into separate folders, ensuring each folder contains only 20 listings. 
Update `listing_path` in `text_extraction.py` and `ProcessingInput:source` in `run_text_extraction.py` to point to your mannually created folders.
Then execute the following commands in SageMaker Studio for accessing AWS CPU instances:
```bash
cd scripts/sagemaker_pipeline/text_extraction
python run_text_extraction.py
```
Upon completion, the extracted data will be stored in your S3 bucket at `s3://your-s3-bucket/CommonCrawl/parsed-data-compressed`. Log files will also be created in the S3 bucket as designated by the `listing_path` in `scripts/sagemaker_pipeline/text_extraction/text_extraction.py`. These logs are crucial for verifying the completeness of the extraction process and can be analyzed to compile statistics, which will be discussed in the following section.

### Checking Running results
The generated log files are in `.csv` format, which could be used to verify that each CC WARC file has been successfully processed and to gather the overall statistics of the text extraction operation. Use the following commands one by one to download and analyze the statistics:
```bash
# Download the statistics files to your local system
aws s3 cp s3://your-s3-bucket/path/to/listings/under/s3/bucket/text_extraction/run-1 raw-stats/text_extraction/run-1  --recursive --exclude "*" --include "*.csv"

# Navigate to the results checking script directory
cd scripts/check_results/text_extraction

# Concatenate statistics from different processors
bash concat.sh

# Check the consolidated statistics
python check_statistics.py
```
It's possible that the running is not completed due to various reasons, including issues like exceeding the Common Crawl S3 bucket's request limit, which prevents downloading CC WARC files to SageMaker nodes, or encountering data transfer limitations from SageMaker nodes to our S3 bucket due to high concurrent requests. In case of any failures, execute the following commands to identify files that failed to process and need re-processing:
```bash
cd scripts/check_results/text_extraction
python gen_rem_file.py  
```
You can then re-run the text extraction process for the remaining files generated above.

---

Upon successful completion of the text extraction process, you will become well-acquainted with the workflow necessary for subsequent processing steps. 
These steps adhere to a similar procedural breakdown: generating listings, executing processes on SageMaker, and reviewing the results. 
The primary distinctions reside in the configurations for generating listings and the specific scripts employed during the processing stages.

## 2. Quality Filtering
### Generating Listings
Running quality filtering on one dump may take around 2800 - 4400 CPU core hours.
Coonsider the same setting as text extraction, i.e., you have 200 CPU nodes with 72 cores each and you want to extract 2 new dumps, 
then you can divide the cc listings into 20 splits by utilizing 20 nodes only, and it will take around 4 to 6 hours to finish.
You can achieve this by running the following command:
```bash
cd scripts/process_listings
bash split_listing_for_quality_filtering.sh
```
After running this script, 20 splits will be generated and stored in `listings/quality_filtering/run-1/`.

Similar as for text extraction, you need to upload the listings to somewhere in our S3 bucket. Let's say, `s3://your-s3-bucket/path/to/listings/under/s3/bucket/quality_filtering/run-1/`.
```bash
aws s3 sync listings/quality_filtering/run-1 s3://your-s3-bucket/path/to/listings/under/s3/bucket/quality_filtering/run-1
```

### Executing Quality Filtering
Next, run the quality filtering on AWS cpu nodes through SageMaker using following commands: 
```bash
cd scripts/sagemaker_pipeline/quality_filtering
python run_quality_filtering.py
```
After the running, extracted data will be stored in your s3 bucket: `s3://your-s3-bucket/CommonCrawl/filtered-data`. 

### Checking Running Results
```bash
# Download the statistics files to your local system
aws s3 cp s3://your-s3-bucket/path/to/listings/under/s3/bucket/quality_filtering/run-1 raw-stats/quality_filtering/run-1  --recursive --exclude "*" --include "*.csv"

# Navigate to the results checking script directory
cd scripts/check_results/quality_filtering

# Concatenate statistics from different processors
bash concat.sh

# Check the consolidated statistics
python check_statistics.py
```

Similarly as for text extraction, run the following command to generate the failed file listings if there are any failed files.
```bash
python gen_rem_file.py  
```

## 3. Local Exact Deduplication
### Generate Listings
Running local exact deduplication on one dump may take around 850 - 1500 CPU core hours.
In our case, we split the listings in each dump into continuous 70 splits and each processor deals with one split. 
Specifically, each processor runs a exact deduplication over all the files within the given split using Bloom Filter.
You can achieve this by running the following command:
```bash
cd scripts/process_listings
bash split_listing_for_local_deduplication.sh
```
After running this script, 140 (if we have 2 dumps) splits will be generated and stored in `listings/local_deduplication/`.

Then upload the listings to somewhere in our S3 bucket. Let's say, "s3://your-s3-bucket/path/to/listings/under/s3/bucket/local_deduplication/".
```bash
aws s3 sync listings/quality_filtering s3://your-s3-bucket/path/to/listings/under/s3/bucket/local_deduplication
```

### Executing Local Deduplication
Next, you could run the local exact deduplication on AWS cpu nodes by SageMaker using following commands: 
```bash
cd scripts/sagemaker_pipeline/local_deduplication
python run_local_deduplication.py
```
After the running, extracted data will be stored in your s3 bucket: `s3://your-s3-bucket/CommonCrawl/local-deduped-data`. 

### Checking Running Results
```bash
# Download the statistics files to your local system
aws s3 cp s3://your-s3-bucket/path/to/listings/under/s3/bucket/local_deduplication/run-1 raw-stats/local_deduplication/run-1  --recursive --exclude "*" --include "*.csv"

# Navigate to the results checking script directory
cd scripts/check_results/local_deduplication

# Concatenate statistics from different processors
bash concat.sh

# Check the consolidated statistics
python check_statistics.py
```
If there is any failed files within one split, you need to restart the local deduplication for the whole split.


## 4. Hash Generation
### Generating Listings
Running hash value generation on one dump may take around 10000 - 15000 CPU core hours.
You can split the listings into 20 splits for each dump and then mannually put the listings for each dump into separate folders. 
Then the hash value generation process could finish around 10 hours with 20 nodes with 72 cores each.
```bash
cd scripts/split_listings
bash split_listings_for_hash_generation.sh
```
After running this script, splits will be generated and stored in `listings/hash_generation/run-1/`.

Then upload the listings into AWS S3 bucket:
```bash
aws s3 sync listings/hash_generation/run-1 s3://your-s3-bucket/path/to/listings/under/s3/bucket/hash_generation/run-1
```

### Hash Value Generation
Next, you could run hash value generation on AWS cpu nodes by SageMaker using following commands: 
```bash
cd scripts/sagemaker_pipeline/hash_generation
python run_hash_generation.py
```

### Checking Running Results
```bash
# Download the statistics files to your local system
aws s3 cp s3://your-s3-bucket/path/to/listings/under/s3/bucket/hash_generation/run-1 raw-stats/hash_generation/run-1  --recursive --exclude "*" --include "*.csv"

# Navigate to the results checking script directory
cd scripts/check_results/hash_generation

# Check the number of completed files for each dump
python check_completed_by_ls.py  

# Concatenate statistics from different processors
bash concat.sh

# Check the consolidated statistics
python check_statistics.py

# # Run the following command to generate the failed file listings if there are any failed files.
# python gen_rem_file.py  
```

## 5. Global Deduplication

This step will be run on large-memory CPUs. The scripts are located at "deduplication" folder (outside "common-crawl"). The output consists of three types of pickle files: one with document IDs that needs to be removed from each file, another with sample connected components, and a third containing metadata about duplicates.

## 6. Duplicates Output Processing
Of the three types of pickle files generated by global deduplicaiton, only two are used for subsequent steps: the document ids that need to be removed for each file and the duplicates meta information.
Across the pickle files, there are many overlapping files, i.e., each pickle file may contain different information about the document ids to remove and duplicates meta for the same file.
To avoid overwhelmed access to the same file, we have to post-process the pickle files and merge the information of same files.

The post-processing could be conducted as split-then-merge:
- **Split** the pickle files based on the file ids of cc files
- **Merge** the information for the same file ids

This post-processing could be performed locally. Our scripts below are used in local slurm cluster.

Before running the scripts below, download the pickle files to your local cluster.

### Split Remove Info
```bash
cd scripts/post_process_dup_results
sbatch sbatch_split_remove.sh
```
### Merge Remove Info
```bash
cd scripts/post_process_dup_results
sbatch sbatch_merge_remove.sh
sbatch sbatch_merge_remove_non-web.sh
```
### Split Meta Info
```bash
cd scripts/post_process_dup_results
sbatch sbatch_split_meta.sh
```
### Merge Meta Info
```bash
cd scripts/post_process_dup_results
sbatch sbatch_merge_meta.sh
sbatch sbatch_merge_meta_non-web.sh
```
### Merge Remove and Meta
```bash
cd scripts/post-process-dup-results
sbatch sbatch_merge_2dict.sh
```

## 7. Duplicates Removal

The merged deduplication results will then be used as "listings" to guide the processors for removing duplicates from files and adding meta information.
So we need to upload the merged pickle files into somewhere in our S3 bucket. 
Let's say, "s3://your-s3-bucket/path/to/listings/under/s3/bucket/remove_duplicates/".
```bash
aws s3 sync duplicates/two-dict-merged/chunk_500 s3://your-s3-bucket/path/to/listings/under/s3/bucket/remove_duplicates/chunk_500/run-1
```

Next, we could run the duplicates removal on AWS cpu nodes by SageMaker using following commands: 
```bash
cd scripts/pipeline
python run_duplicates_removal.py
```

There may be several files which are not contained in the deduplication results, due to that they have no duplicated documents to remove or duplicates meta to add, or they are just empty files. To find out all these files to avoid any data missing, you need to generate a list for the remaining files. For these files, you can simply do copy action to copy them from previous directory to your target data directory.

To start, you need to generate a full list of all the files we have via the following command:
``` bash
cd scripts/get_hash_map/
python generate_dump2files.py
```
Then, you can generate the list for the remaining files by running the following command:
```bash
cd scripts/poat_process_dup_results
python gen_files_without_dup.py
```
A "remaining_files.txt" will be generated in `scripts/poat_process_dup_results`. You can upload it to the S3 bucket and then run the following command in SageMaker studio:
```bash
cd scripts/sagemaker_pipiline/duplicates_removal/
python run_duplicates_removal_for_remaining.py
```

### Checking Running Results
```bash
# Download the statistics files to your local system
aws s3 cp s3://your-s3-bucket/path/to/listings/under/s3/bucket/duplicates_removal/chunk_500/run-1 raw-stats/duplicates_removal/chunk_500/run-1  --recursive --exclude "*" --include "*.csv"

# Navigate to the results checking script directory
cd scripts/check_results/duplicates_removal

# Check the number of completed files for each dump
python check_completed_by_ls.py  

# Concatenate statistics from different processors
bash concat.sh

# Check the consolidated statistics
python check_statistics.py

# Check the statistics for the files without duplicates information
python check_statistics_for_remaining.py

# # Run the following command to generate the failed file listings if there are any failed files.
# python gen_rem_file.py  
```

## 8. Re-Organize Data
For easier usage of our data, we re-organize our data based on the duplicates information. 
The re-organizing is implemented via split-then-merge. 

### Splitting
```bash
# Generate listings for splitting
cd scripts/process_listings
bash split_listing_for_re-organizing_data_split.sh
# Upload the listing into s3 bucket
aws s3 sync ../../listings/re-organizing_data/split/run-1 s3://your-s3-bucket/path/to/listings/under/s3/bucket/re-organizing_data/split/run-1
```

Then run the splitting on SageMaker studio.
```bash
cd scripts/sagemaker_pipeline/re-organize_data
python run_split.py
```

Then check the running results.
```bash
# Download the statistics to local system
aws s3 cp s3://your-s3-bucket/path/to/listings/under/s3/bucket/re-organizing_data/split/run-1-log raw-stats/re-organizing_data/split/run-1 --recursive --exclude "*" --include "*.csv"

cd scripts/check_results/re-organizing_data/split

# Concatenate statistics from different nodes and processors
bash concat.sh
# Transfer the format in the csv files
python change_csv_format.py
# Get the statistics of duplicates distribution and check the results
python check_statistics.py
```
### Merging
```bash
# Generate different listings for merging of different intervals to achieve a reasonable running
cd scripts/process_listings
bash split_listing_for_re-organizing_data_merge.sh
# Upload the listing into s3 bucket
aws s3 sync ../../listings/re-organizing_data/merge/run-1 s3://your-s3-bucket/path/to/listings/under/s3/bucket/re-organizing_data/merge/run-1
```

Then run the merging on SageMaker studio. Note that you need to copy the merge scripts multiple times so that one script deals with one duplicate interval.
```bash
cd scripts/sagemaker_pipeline/re-organize_data
python run_merge.py
```

It's possible that you may meet some failed files due to the frequent access to the s3 bucket.
```bash
# Download the failed file listings into local system
aws s3 cp s3://your-s3-bucket/path/to/listings/under/s3/bucket/re-organizing_data/merge/run-1/1-1-log raw-stats/re-organizing_data/merge/run-1-failed/1-1 --recursive --exclude "*" --include "*.failed.txt"  # repeat for other intervals

# Concatenate the failed files by dumps --> for new running
cd scripts/check_results/re-organizing_data/merge
python concat_failed.py
```

In most cases, the error may comes from the splitting stage. So you may need to re-run the splitting on SageMaker studio
```bash
cd scripts/sagemaker_pipeline/re-organize_data
python run_split_for_failed.py
```

Then run the merging on SageMaker studio. 
```bash
cd scripts/sagemaker_pipeline/re-organize_data
python run_merge_for_failed.py  # modify the `split_id` in merge_data_for_failed_id if you have more than 10 listings for each dump in the 1st running
```

Then check the running results again.
```bash
# Download the statistics to local system
aws s3 cp s3://your-s3-bucket/path/to/listings/under/s3/bucket/re-organizing_data/merge/run-1/1-1-log raw-stats/re-organizing_data/merge/run-1/1-1 --recursive --exclude "*" --include "*.csv"

# Concatenate statistics from different processors
bash concat.sh

# Check the consolidated statistics
python check_statistics.py
```

Then you are done!