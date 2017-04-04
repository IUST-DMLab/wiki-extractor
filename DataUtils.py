import string
from os.path import join

import Config
from Utils import logging_file_opening, logging_file_closing


def open_extracted_bz2_dump_file(extracted_pages_counter, output_dir):
    file_number = str(int(extracted_pages_counter / Config.extracted_pages_per_file))
    extracted_pages_filename = join(output_dir, file_number)
    extracted_pages_file = open(extracted_pages_filename, 'w+')
    logging_file_opening(extracted_pages_filename)
    return extracted_pages_filename, extracted_pages_file


def close_extracted_bz2_dump_file(extracted_pages_filename, extracted_pages_file):
    extracted_pages_file.close()
    logging_file_closing(extracted_pages_filename)


def is_infobox_file(filename):
    return True if 'infoboxes' in filename else False


def is_revision_ids_file(filename):
    return True if 'revision_ids' in filename else False


def get_infoboxes_filename(prefix):
    return prefix+'-infoboxes'


def get_revision_ids_filename(prefix):
    return prefix+'-revision_ids'


def get_wiki_texts_filename(prefix):
    return prefix+'-wiki_texts'


def get_abstracts_filename(prefix):
    return prefix+'-abstracts'


def get_redirects_filename(prefix):
    return prefix+'-redirects'


def get_categories_filename(prefix):
    return prefix+'-categories'


def get_external_links_filename(prefix):
    return prefix+'-external_links'


def get_wiki_links_filename(prefix):
    return prefix+'-wiki_links'


def get_infobox_lang(infobox_type):
    if infobox_type in Config.infobox_flags_en:
        return 'en'
    elif infobox_type in Config.infobox_flags_fa:
        return 'fa'


def is_ascii(sentence):
    try:
        sentence.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False


def without_en_chars(sentence):
    return all(c not in string.ascii_letters for c in sentence)
