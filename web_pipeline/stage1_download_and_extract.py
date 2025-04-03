import gc
import multiprocessing
import os
import random
import time
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, fields

import fasttext
from datatrove.data import Document
from fastwarc.stream_io import *
from fastwarc.warc import ArchiveIterator, WarcRecordType
from lxml import html
from prettytable import PrettyTable
from resiliparse.parse.encoding import bytes_to_str, detect_encoding
from tqdm import tqdm
from url_filtering.url_filter import CustomURLFilterWithWhitelist
from utils.datatrove_utils import TxtReader
from utils.file_utils import (delete_local_files, download_from_cc, make_dir,
                              remove_file, write_stat, write_to_jsonlgz)
from utils.latex_parsing import (extract_plain_text,
                                 improve_latex_content_parsing)
from utils.math_fasttext import MathFastTextClassifier


@dataclass
class TextExtractionStatistics:
    file_path: str = None
    doc_input: int = 0
    url_filtered: int = 0
    html_content_empty: int = 0
    html_decoding_failed: int = 0
    doc_extraction_failed: int = 0
    doc_extraction_empty: int = 0
    language_filtered: int = 0
    math_retained: int = 0
    doc_remaining: int = 0


def html_table_to_ascii(headers, rows):
    ascii_table = PrettyTable()
    if headers:
        ascii_table.field_names = headers
    else:
        raise ValueError("Headers cannot be empty when using PrettyTable.")

    for row in rows:
        # check if row is empty, and ensure the number of columns is consistent
        if row and len(row) != len(headers):
            raise ValueError(
                f"Row has incorrect number of values: {len(row)} != {len(headers)}"
            )
        ascii_table.add_row(row)

    return ascii_table.get_string()


def html_table_to_markdown(headers, rows):
    markdown_table = []
    if headers:
        markdown_table.append("| " + " | ".join(headers) + " |")
        markdown_table.append("| " + " | ".join(["---"] * len(headers)) + " |")
    for row in rows:
        if row and any(cell.strip() for cell in row):  # 确保行不是空的
            formatted_row = [cell.replace("\n", " ").strip() for cell in row]
            markdown_table.append("| " + " | ".join(formatted_row) + " |")
    return "\n".join(markdown_table)


def random_table_converter(table_element, format_choice=None):
    format_choice = format_choice or random.choice(["ascii", "markdown"])
    headers = [th.text_content().strip() for th in table_element.xpath(".//th")] or []
    rows = [
        [td.text_content().strip() for td in tr.xpath(".//td")]
        for tr in table_element.xpath(".//tr")
        if tr.xpath(".//td")
    ]
    if format_choice == "ascii":
        return html_table_to_ascii(headers, rows)
    else:
        return html_table_to_markdown(headers, rows)


def process_tables(tree, format_choice=None):
    if not isinstance(tree, html.HtmlElement):
        raise TypeError("Expected an lxml.html.HtmlElement object")
    for table in tree.xpath("//table"):
        table_text = random_table_converter(table, format_choice=format_choice)
        new_element = html.Element("pre", attrib={"class": "converted-table"})
        new_element.text = f"\n{table_text}\n"
        table.getparent().replace(table, new_element)
    return tree


FIELD_NAMES = [field.name for field in fields(TextExtractionStatistics)]


def parse_args() -> Namespace:
    """Parse command-line arguments for processing WARC files."""
    parser = ArgumentParser(
        description="Parse WARC files to extract textual content for language identification."
    )
    parser.add_argument(
        "--listing_path",
        type=str,
        default="./sample_listings/",
        help="directory path where listing files are stored.",
    )
    parser.add_argument(
        "--listing_file",
        type=str,
        default=None,
        help="Specific listing file name to process. Default is None, implying processing all files in listing_path.",
    )
    parser.add_argument(
        "--root_path",
        type=str,
        default="./",
        help="Root directory where downloaded and processed data should be stored.",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=4,
        help="Number of parallel processes to use for data processing.",
    )
    parser.add_argument(
        "--node_rank", type=int, default=0, help="rank of the current CPU node."
    )
    parser.add_argument(
        "--total_nodes", type=int, default=1, help="Total number of nodes."
    )
    parser.add_argument(
        "--glob_pattern",
        type=str,
        default="2024-46.Split_199.txt",
        help="Glob pattern to match the listing files.",
    )
    # @fan
    # so, for failed cases, we don't want to overwrite the existing math_data and extracted_data
    # this way, we should set this flag to True
    parser.add_argument(
        "--no_overwrite",
        action="store_true",
        help="Whether to overwrite the existing files.",
    )
    # some abs path configs
    parser.add_argument(
        "--warc_data_path",
        type=str,
        default="./listings/text_extraction/run-1/",
        help="Root directory where downloaded and processed data should be stored.",
    )
    parser.add_argument(
        "--extracted_data_path",
        type=str,
        default="./extracted_data/",
        help="Root directory where extracted data should be stored.",
    )
    parser.add_argument(
        "--math_data_path",
        type=str,
        default="./math_data_joint_pipeline_050_filtered/",
        help="Root directory where math data should be stored.",
    )
    parser.add_argument(
        "--failed_downloads_path",
        type=str,
        default="./failed_downloads/",
        help="Root directory where downloaded and processed data should be stored.",
    )
    return parser.parse_args()


args = parse_args()


# Define the math fastText classifier
LABEL_PREFIX = "__label__"
LABEL_MATH = f"{LABEL_PREFIX}MATH"
LABEL_NON_MATH = f"{LABEL_PREFIX}NON_MATH"
MATH_THRESHOLD = 0.5
math_classifier = MathFastTextClassifier(
    model_path="models/math_classifier.bin",
    math_threshold=MATH_THRESHOLD,
    math_class_name=LABEL_MATH,
)

url_filter = CustomURLFilterWithWhitelist(
    use_whitelist=True,
    do_load_from_cache=True,
    do_remove_curated_sources=True,
    do_add_extra_domain_and_urls=True,
    exclusion_writer=None,
)


class LanguageIdentification:
    """A class to handle language identification using a pre-trained FastText model."""

    def __init__(self, pretrained_lang_model):
        """Initialize with a pre-trained language model."""
        self.model = fasttext.load_model(pretrained_lang_model)

    def predict_lang(self, text, k=1):
        """Predict the top k language(s) of the given text."""
        labels, scores = self.model.predict(text, k)
        labels = [label.replace("__label__", "") for label in labels]
        return labels, scores


# Preload language identification model
LANGUAGE = LanguageIdentification(pretrained_lang_model="models/lid.176.bin")


def process_file(input_file):
    gc.collect()

    # Get the process id
    pid = os.getpid()

    # random sleep
    # @fan: uncomment this line when networking is not stable
    # time.sleep(random.randint(1, 15))

    ############################################################
    # download the warc file from AWS S3 or HTTPS
    ############################################################
    local_input_file = download_from_cc(input_file, local_root_path=args.root_path)

    # we record the failed downloads for future re-downloading
    if local_input_file is None:
        save_name = args.glob_pattern.replace("*", "")
        os.makedirs(args.failed_downloads_path, exist_ok=True)
        with open(
            f"{args.failed_downloads_path}/{save_name}_download_failed.txt",
            "a",
        ) as f:
            f.write(f"{input_file}\n")
        return

    ############################################################
    # Setup output file path
    ############################################################

    # @fan: expect to produce same filename for:
    # 1. extracted_data and math_data
    # 2. for segments and html_en/ under folders in 1.
    fake_output_file = local_input_file.replace(
        "crawl-data", args.extracted_data_path
    ).replace(".gz", ".jsonl.gz")
    math_output_file = local_input_file.replace(
        "crawl-data", args.math_data_path
    ).replace(".gz", ".jsonl.gz")

    math_output_text_file = math_output_file.replace("segments", "segments_en")
    math_output_html_file = math_output_file.replace("segments", "html_en")

    en_output_text_file = fake_output_file.replace("segments", "segments_en")
    en_output_html_file = fake_output_file.replace("segments", "html_en")

    print("Output file: ", en_output_text_file)

    ############################################################
    # Ensure output directories exist
    ############################################################

    # Re-create to ensure output directory exists
    make_dir(math_output_text_file)
    make_dir(math_output_html_file)
    # remove the existing files if no-overwrite is False
    if not args.no_overwrite:
        remove_file(math_output_text_file)
        remove_file(math_output_html_file)

    make_dir(en_output_text_file)
    make_dir(en_output_html_file)
    # remove the existing files if no-overwrite is False
    if not args.no_overwrite:
        remove_file(en_output_text_file)
        remove_file(en_output_html_file)

    ############################################################
    # Setup paths for statistics
    ############################################################
    if args.listing_file:
        stat_file = args.listing_file.replace(
            ".txt", f"-log/{pid}.statistics.csv"
        ).replace("listings", "logs")
    else:
        save_name = args.glob_pattern.replace("*", "")
        stat_file = "./stats/" + save_name.replace(
            ".txt", f"-log/{pid}.statistics.csv"
        ).replace("listings", "logs")
    make_dir(stat_file)
    # Prepare to collect extraction statistics
    statistics = TextExtractionStatistics()
    statistics.file_path = input_file

    ############################################################
    # Read, filter and extract the warc file
    ############################################################
    stream = GZipStream(FileStream(local_input_file, "rb"))

    en_data_out_html, en_data_out_text = [], []
    math_data_out_html, math_data_out_text = [], []

    for record_idx, record in enumerate(
        tqdm(ArchiveIterator(stream, record_types=WarcRecordType.response))
    ):
        statistics.doc_input += 1

        ############################################################
        # Read url and html content from warc format data
        ############################################################
        url = record.headers.get("WARC-Target-URI")
        fake_doc = Document(
            text="just random text", id=str(int(time.time())), metadata={"url": url}
        )
        try:
            # try to filter via url domain
            url_flag = url_filter.filter(fake_doc)
            if url_flag != True:
                statistics.url_filtered += 1
                continue
        except Exception as e:
            # for any url filter error, skip this operation, keep the document for further processing
            pass

        ############################################################
        # Read html content from warc format data
        ############################################################
        time_stamp = record.headers.get("WARC-Date")
        content_stream = record.reader.read()

        if not content_stream:
            statistics.html_content_empty += 1
            continue

        # Decode the response bytes
        html_doc = bytes_to_str(content_stream, detect_encoding(content_stream))

        if not html_doc:
            statistics.html_decoding_failed += 1
            continue

        ############################################################
        # Extract texts from the content
        ############################################################
        try:

            # ⚠️⚠️⚠️ reformat the html text by improving latex content rendering
            reformatted_html_doc = improve_latex_content_parsing(html_doc)
            try:
                tree = html.fromstring(reformatted_html_doc)
                tree = process_tables(tree)
                reformatted_html_doc = html.tostring(
                    tree, encoding="unicode", pretty_print=True
                )
            except Exception as e:
                # print(f"Raise an exception {e} when dealing with tables")
                pass

            # ⚠️⚠️⚠️ extract texts from the html documents
            result = extract_plain_text(
                reformatted_html_doc,
                alt_texts=False,
                links=False,
                preserve_formatting=True,
            )

        except:
            statistics.doc_extraction_failed += 1
            print(f"Extraction error on {input_file}")
            continue

        if not result:
            statistics.doc_extraction_empty += 1
            continue

        ############################################################
        # Perform language identification
        ############################################################
        lang_labels, lang_scores = LANGUAGE.predict_lang(result.replace("\n", " "))

        # ⚠️⚠️⚠️ perform language identification
        # if lang_labels[0] != "en" or lang_scores[0] < 0.65:
        if (lang_labels[0] not in ["en"]) or lang_scores[0] < 0.65:
            statistics.language_filtered += 1
            continue

        # Define output format
        out_text_json = {
            "text": result,
            "meta": {
                "lang": lang_labels[0],
                "lang_score": lang_scores[0],
                "url": url,
                "timestamp": time_stamp,
                "cc-path": input_file,
                "record_idx": record_idx,
                "math_score": 0.0,
            },
        }

        out_html_json = {
            "text": html_doc,
            "meta": {
                "lang": lang_labels[0],
                "lang_score": lang_scores[0],
                "url": url,
                "timestamp": time_stamp,
                "cc-path": input_file,
                "record_idx": record_idx,
                "math_score": 0.0,
            },
        }

        if lang_labels[0] == "en":
            # en_data_out_html.append(out_html_json)
            en_data_out_text.append(out_text_json)
        statistics.doc_remaining += 1

        ############################################################
        # Perform math classification
        ############################################################
        if lang_labels[0] == "en":
            retain_flag, math_score = math_classifier.predict(result)
            out_text_json["meta"]["math_score"] = math_score
            out_html_json["meta"]["math_score"] = math_score

            if retain_flag:
                math_data_out_text.append(out_text_json)
                math_data_out_html.append(out_html_json)
            if math_score >= 0.80:
                statistics.math_retained += 1

    # Write the output into file
    # assert len(en_data_out_html) == len(en_data_out_text)
    # write_to_jsonlgz(en_data_out_html, en_output_html_file)
    write_to_jsonlgz(en_data_out_text, en_output_text_file)

    # assert len(math_data_out_text) == len(math_data_out_html)
    write_to_jsonlgz(math_data_out_html, math_output_html_file)
    write_to_jsonlgz(math_data_out_text, math_output_text_file)

    # Write the statistics into file
    write_stat(stat_file, statistics, input_file, FIELD_NAMES)

    # Remove local files
    delete_local_files(
        [
            local_input_file,
        ]
    )
    gc.collect()
    time.sleep(random.randint(1, 10))


if __name__ == "__main__":
    # Get the list of files that need to be processed
    files_list = []
    # only for testing
    if args.listing_file:
        with open(args.listing_file) as f:
            for line in f:
                files_list.append(line.strip())

        # Print sample files that are going to be processed
        print("Files to process: ")
        print(files_list[:4])
        # files_list = files_list[:100]

        # Process the documents in parallel
        with multiprocessing.Pool(args.processes) as pool:
            pool.map(process_file, files_list)

    # iterate over all the listing files in the listing path
    else:
        print(args.glob_pattern)
        txt_reader = TxtReader(
            data_folder=args.warc_data_path,
            glob_pattern=args.glob_pattern,
            doc_progress=True,
        )
        files_list = []
        for idx, file_path in enumerate(
            txt_reader.run(rank=args.node_rank, world_size=args.total_nodes)
        ):
            files_list.append(file_path.text.replace("\n", ""))

        # time count
        start_time = time.time()

        # Process the documents in parallel
        with multiprocessing.Pool(args.processes) as pool:
            pool.map(process_file, files_list)

        elapsed_time = time.time() - start_time
        print(f"Total time taken: {elapsed_time:.2f} seconds")
