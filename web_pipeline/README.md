# Web Pipeline

This folder contains the code for the web pipeline.
First please follow instructions in [download](./download/download.md) folder to get all the available WARC file paths.

## Stage 1: Download and Extract
This will download the WARC files from the Common Crawl and extract the text and HTML content. Meanwhile, we will perform language identification and math text filtering using fasttext models.

```bash
python stage1_download_and_extract.py
```

## Stage 2: Deduplication

We mainly follow DataTrove's example to perform deduplication.
Please refer to the example code in [datatrove](https://github.com/huggingface/datatrove/blob/main/examples/minhash_deduplication.py) for more details. The majority of the code is the same, but we use a different bucket size and hash function number (11 , 10).

## Stage 3: Re-extraction

TODO