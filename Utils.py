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


def is_infobox_file(filename):
    return True if 'infoboxes' in filename else False


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


def get_infobox_lang(infobox_name):
    if infobox_name in Config.infobox_flags_en:
        return 'en'
    elif infobox_name in Config.infobox_flags_fa:
        return 'fa'
    else:
        return None


def get_wiki_article_in_template_statistic_sql_dump(rows, order):
    table_name = 'wiki_article_in_template_statistic'
    command = "INSERT INTO `%s` VALUES " % table_name
    for i, row in enumerate(rows):
        command += "('%s','%s','%s',%s)," % (row[order[0]].replace("'", "''"), row[order[1]].replace("'", "''"),
                                             row[order[2]].replace("'", "''"), row[order[3]])
        if (i+1) % 100 == 0:
            command = command[:-1] + ";\nINSERT INTO `%s` VALUES " % table_name

    command = command[:-1] + ";"
    return command


def get_wiki_template_redirect_sql_dump(redirects):
    table_name = 'wiki_template_redirect'
    command = "INSERT INTO `%s` VALUES " % table_name
    for i, redirect_from in enumerate(redirects):
        redirect_to = redirects[redirect_from]
        command += "('%s','%s')," % (redirect_from.replace("'", "''"), redirect_to.replace("'", "''"))
        if (i+1) % 100 == 0:
            command = command[:-1] + ";\nINSERT INTO `%s` VALUES " % table_name

    command = command[:-1] + ";"
    return command


def save_sql_dump(directory, filename, sql_dump, encoding='utf8'):
    if len(directory) < 255:
        create_directory(directory)
        sql_filename = join(directory, filename)
        sql_file = open(sql_filename, 'w+', encoding=encoding)
        sql_file.write(sql_dump)
        sql_file.close()


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


def save_json(directory, filename, dict_to_save, filter_dict=None, encoding='utf8', sort_keys=True):
    if filter_dict:
        dict_to_save = dict((key, value) for key, value in dict_to_save.items()
                            if key in filter_dict)
    if len(directory) < 255:
        create_directory(directory)
        information_filename = get_information_filename(directory, filename)
        json_file = open(information_filename, 'w+', encoding=encoding)
        json_dumps = json.dumps(dict_to_save, ensure_ascii=False, indent=2, sort_keys=sort_keys)
        json_file.write(json_dumps)
        json_file.close()


def load_json(directory, filename, encoding='utf8'):
    information_filename = get_information_filename(directory, filename)
    information_file = open(information_filename, encoding=encoding).read()
    information = json.loads(information_file, encoding=encoding)
    return information


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


# leila 1.12.95
def detect_language(s):
    try:
        s.encode('ascii')
    except UnicodeEncodeError:
        return 'fa'
    else:
        return 'en'


def get_fa_infoboxes_names():
    fa_infobox_path = {}
    for path in os.listdir(Config.extracted_pages_path_with_infobox_dir):
        with open(join(Config.extracted_pages_path_with_infobox_dir, path)) as f:
            data = json.load(f)
            fa_infobox_path = {k: v for k, v in data.items() if k.startswith(tuple(Config.infobox_flags_fa))}
    return fa_infobox_path
