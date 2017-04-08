import bz2
import json
import logging
import os
import string
from collections import defaultdict
from genericpath import exists
from os.path import join

from alphabet_detector import AlphabetDetector
from bs4 import BeautifulSoup

import Config
import LogUtils
from ThirdParty.WikiCleaner import clean


def open_extracted_bz2_dump_file(extracted_pages_counter, output_dir, lang):
    file_number = str(int(extracted_pages_counter / Config.extracted_pages_per_file[lang]))
    extracted_pages_filename = join(output_dir, file_number)
    extracted_pages_file = open(extracted_pages_filename, 'w+')
    LogUtils.logging_file_opening(extracted_pages_filename)
    return extracted_pages_filename, extracted_pages_file


def close_extracted_bz2_dump_file(extracted_pages_filename, extracted_pages_file):
    extracted_pages_file.close()
    LogUtils.logging_file_closing(extracted_pages_filename)


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


def get_json_filename(directory, filename):
    filename = str(filename).replace('.json', '')
    return join(directory, filename+'.json')


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

    LogUtils.logging_file_opening(filename)

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
    LogUtils.logging_file_closing(filename)


def parse_page(xml_page):
    soup = BeautifulSoup(xml_page, "xml")
    page = soup.find('page')
    return page


def is_fa_infobox(infobox_name):
    if any(flag in infobox_name for flag in Config.infobox_flags_fa):
        return True
    return False


def get_fawiki_fa_infoboxes():
    directory = Config.extracted_pages_with_infobox_dir['fa']
    fawiki_fa_infoboxes = defaultdict(list)
    for filename in os.listdir(directory):
        pages_infoboxes = load_json(directory, filename)
        for page_name, infoboxes in pages_infoboxes.items():
            for infobox in infoboxes:
                if is_fa_infobox(infobox):
                    fawiki_fa_infoboxes[page_name].append(infobox)
    return fawiki_fa_infoboxes


def get_enwiki_infoboxes():
    directory = Config.extracted_pages_with_infobox_dir['en']
    enwiki_infoboxes = defaultdict(list)
    for filename in os.listdir(directory):
        enwiki_infoboxes.update(load_json(directory, filename))

    return enwiki_infoboxes


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


def get_disambiguation_links_regular(sentences):
    sentences = sentences.splitlines()
    disambiguation_links = list()

    for sentence in sentences:
        if Config.see_also_in_fa in sentence:
            break

        disambiguation_regex_match_result = Config.disambiguation_regex.match(sentence)
        if not (str(disambiguation_regex_match_result) == 'None'):
            start_index = sentence.find('[[')
            end_index = sentence.find(']]')
            disambiguation_links.append(sentence[start_index+2:end_index])

    return disambiguation_links
