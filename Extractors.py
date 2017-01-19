import os
from os.path import join
from joblib import Parallel, delayed
import Config
import Utils

extracted_pages_files = os.listdir(Config.extracted_pages_dir)


def extract_wikipedia_page():
    Utils.extract_wikipedia_pages(Config.fawiki_latest_pages_articles_dump)


def extract_infoboxes():
    Parallel(n_jobs=-1)(delayed(Utils.extract_infoboxes)
                        (join(Config.extracted_pages_dir, file_name),
                         Utils.get_information_filename(Config.extracted_infoboxes_dir, file_name))
                        for file_name in extracted_pages_files)


def extract_ids():
    Parallel(n_jobs=-1)(delayed(Utils.extract_ids)
                        (join(Config.extracted_pages_dir, file_name),
                         Utils.get_information_filename(Config.extracted_ids_dir, file_name))
                        for file_name in extracted_pages_files)
