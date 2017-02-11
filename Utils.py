import bz2
import csv
import gzip
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


def save_dict_to_json_file(filename, dict_to_save, encoding='utf8'):
    json_file = open(filename, 'w+', encoding=encoding)
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


def find_sql_records(line):
    records_str = line.partition('` VALUES ')[2]
    records_str = records_str.strip()[1:-2]
    records = records_str.split('),(')
    return records


def get_sql_rows(file_name, encoding='utf8'):
    with gzip.open(file_name, 'rt', encoding=encoding, errors='ignore') as f:
        logging_file_operations(file_name, 'Opened')
        for line in f:
            if line.startswith('INSERT INTO '):
                all_records = find_sql_records(line)
                for record in all_records:
                    yield csv.reader([record], delimiter=',', doublequote=False,
                                     escapechar='\\', quotechar="'", strict=True)
    logging_file_operations(file_name, 'Closed')
