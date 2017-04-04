import bz2
import json
import logging
import os
from collections import defaultdict
from os.path import join, exists

from alphabet_detector import AlphabetDetector
from bs4 import BeautifulSoup

import Config
from ThirdParty.WikiCleaner import clean

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s ', level=logging.DEBUG)


def logging_file_operations(filename, operation):
    logging.info('%s %s!' % (filename, operation))


def logging_file_opening(filename):
    logging_file_operations(filename, 'Opened')


def logging_file_closing(filename):
    logging_file_operations(filename, 'Closed')


def logging_pages_extraction(pages_number, filename):
    logging.info('%d Pages Extracted from %s!' % (pages_number, filename))


def logging_information_extraction(pages_number, filename):
    logging.info('%d Pages Checked from %s!' % (pages_number, filename))


def get_json_filename(directory, filename):
    filename = str(filename).replace('.json', '')
    return join(directory, filename+'.json')


def is_fa_infobox(infobox_name):
    if any(flag in infobox_name for flag in Config.infobox_flags_fa):
        return True
    return False


def save_json(directory, filename, dict_to_save, filter_dict=None, encoding='utf8', sort_keys=True):
    if filter_dict:
        dict_to_save = dict((key, value) for key, value in dict_to_save.items()
                            if key in filter_dict)
    if len(directory) < 255:
        create_directory(directory)
        information_filename = get_json_filename(directory, filename)
        json_file = open(information_filename, 'w+', encoding=encoding)
        json_dumps = json.dumps(dict_to_save, ensure_ascii=False, indent=2, sort_keys=sort_keys)
        json_file.write(json_dumps)
        json_file.close()


def load_json(directory, filename, encoding='utf8'):
    information_filename = get_json_filename(directory, filename)
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

    logging_file_opening(filename)

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
    logging_file_closing(filename)


def parse_page(xml_page):
    soup = BeautifulSoup(xml_page, "xml")
    page = soup.find('page')
    return page


def detect_language(s):
    try:
        replace_list = ['–', '•']
        for x in replace_list:
            if x in s:
                s = s.replace(x, '')
        s.encode('ascii')

    except UnicodeEncodeError:
        ad = AlphabetDetector()
        lang = ad.detect_alphabet(s)
        if not ('ARABIC' in lang):
            return 'other'
        return 'fa'
    else:
        return 'en'


def get_fa_infoboxes_per_pages():
    fa_infoboxes_per_pages = defaultdict(list)
    for filename in os.listdir(Config.extracted_pages_with_infobox_dir):
        data = load_json(Config.extracted_pages_with_infobox_dir, filename)
        for fa_wiki_page, page_infoboxes in data.items():
            for infobox in page_infoboxes:
                if is_fa_infobox(infobox):
                    fa_infoboxes_per_pages[fa_wiki_page].append(infobox)
    return fa_infoboxes_per_pages


def get_template_name_type(template_name):
    template_name = clean(str(template_name)).replace('template:', '').replace('Template:', '')
    template_name = template_name.replace('الگو:', '').replace('_', ' ')
    template_type = 'template'
    lang = detect_language(template_name)

    lower_template_name = template_name.lower()
    if any(t in lower_template_name for t in Config.infobox_flags_fa+Config.infobox_flags_en):
        template_type = next(t for t in Config.infobox_flags_fa+Config.infobox_flags_en if t in lower_template_name)

    if any(s in lower_template_name for s in Config.stub_flag_fa+Config.stub_flag_en):
        template_type = next(s for s in Config.stub_flag_fa+Config.stub_flag_en if s in lower_template_name)

    return template_name, template_type, lang


def get_infobox_name_type(template_name):
    template_name, infobox_type, lang = get_template_name_type(template_name)
    if infobox_type not in Config.infobox_flags_fa+Config.infobox_flags_en:
        infobox_type = None

    return template_name, infobox_type
