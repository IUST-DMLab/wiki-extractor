import bz2
import json
import logging
import os
from os.path import join, exists

from bs4 import BeautifulSoup

import Config
from ThirdParty.WikiCleaner import clean

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s ', level=logging.DEBUG)


def logging_file_operations(filename, operation):
    logging.info('%s %s!' % (filename, operation))


def logging_pages_extraction(pages_number, filename):
    logging.info('%d Pages Extracted from %s!' % (pages_number, filename))


def logging_information_extraction(pages_number, filename):
    logging.info('%d Pages Checked from %s!' % (pages_number, filename))


def logging_database(msg):
    logging.exception('%s Error!' % msg)


def loggin_id_mapping_error(page_id, func_name):
    logging.info('Id %d mapping NOT FOUND, in %s function!' % (page_id, func_name))


def get_information_filename(info_dir, file_number):
    return join(info_dir, str(file_number)+'.json')


def find_get_infobox_name_type(template_name):
    template_name = clean(str(template_name).lower().replace('_', ' '))
    for name in Config.infobox_flags_en:
        if name in template_name:
            infobox_name = name
            infobox_type = template_name.replace(name, '').strip()
            if not infobox_type:
                infobox_type = 'NULL'
            return infobox_name, infobox_type

    for name in Config.infobox_flags_fa:
        if name in template_name:
            infobox_name = name
            infobox_type = template_name.replace(name, '').strip()
            if not infobox_type:
                infobox_type = 'NULL'
            return infobox_name, infobox_type

    return None, None


def save_dict_to_json_file(filename, dict_to_save):
    json_file = open(filename, 'w+', encoding='utf8')
    json_dumps = json.dumps(dict_to_save, ensure_ascii=False, indent=2, sort_keys=True)
    json_file.write(json_dumps)
    json_file.close()


def create_directory(directory, show_logging=False):
    if not exists(directory):
        if show_logging:
            logging.info(' Create All Directories in Path %s' % directory)
        os.makedirs(directory)


def get_wikipedia_pages(filename):
    if 'bz2' not in filename:
        input_file = open(filename, 'r+')
    else:
        input_file = bz2.open(filename, mode='rt', encoding='utf8')

    logging_file_operations(filename, 'Opened')

    while True:
        l = input_file.readline()
        page = list()
        if not l:
            break
        if l.strip() == '<page>':
            page.append(l)
            while l.strip() != '</page>':
                l = input_file.readline()
                page.append(l)
        if page:
            yield '\n'.join(page)
        del page

    input_file.close()
    logging_file_operations(filename, 'Closed')


def parse_page(xml_page):
    soup = BeautifulSoup(xml_page, "xml")
    page = soup.find('page')
    return page
