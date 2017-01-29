import os
from os.path import join

from joblib import Parallel, delayed

import BZ2_dums_extractor
import Config
import Utils


def extract_infoboxes():
    extracted_pages_files = os.listdir(Config.extracted_pages_articles_dir)
    Utils.create_directory(Config.extracted_infoboxes_dir)
    extracted_infoboxes_files = os.listdir(Config.extracted_infoboxes_dir)
    if extracted_pages_files and not extracted_infoboxes_files:
        Parallel(n_jobs=-1)(delayed(BZ2_dums_extractor.extract_infoboxes)
                            (join(Config.extracted_pages_articles_dir, file_name),
                             Utils.get_information_filename(Config.extracted_infoboxes_dir, file_name))
                            for file_name in extracted_pages_files)


def extract_abstracts():
    extracted_pages_files = os.listdir(Config.extracted_pages_articles_dir)
    Utils.create_directory(Config.extracted_abstracts_dir)
    extracted_abstracts_files = os.listdir(Config.extracted_abstracts_dir)
    if extracted_pages_files and not extracted_abstracts_files:
        Parallel(n_jobs=-1)(delayed(BZ2_dums_extractor.extract_abstracts)
                            (join(Config.extracted_pages_articles_dir, file_name),
                             Utils.get_information_filename(Config.extracted_abstracts_dir, file_name))
                            for file_name in extracted_pages_files)


def extract_ids():
    extracted_pages_files = os.listdir(Config.extracted_pages_meta_current_dir)
    Utils.create_directory(Config.extracted_ids_dir)
    extracted_ids_files = os.listdir(Config.extracted_ids_dir)
    if extracted_pages_files and not extracted_ids_files:
        Parallel(n_jobs=-1)(delayed(BZ2_dums_extractor.extract_ids)
                            (join(Config.extracted_pages_meta_current_dir, file_name),
                             Utils.get_information_filename(Config.extracted_ids_dir, file_name))
                            for file_name in extracted_pages_files)


def extract_revision_ids():
    extracted_pages_files = os.listdir(Config.extracted_pages_articles_dir)
    Utils.create_directory(Config.extracted_revision_ids_dir)
    extracted_revision_ids_files = os.listdir(Config.extracted_revision_ids_dir)
    if extracted_pages_files and not extracted_revision_ids_files:
        Parallel(n_jobs=-1)(delayed(BZ2_dums_extractor.extract_revision_ids)
                            (join(Config.extracted_pages_articles_dir, file_name),
                             Utils.get_information_filename(Config.extracted_revision_ids_dir, file_name))
                            for file_name in extracted_pages_files)
