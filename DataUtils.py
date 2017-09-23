import bz2
import json
import logging
import os
import re
import string
import shutil
from collections import defaultdict
from genericpath import exists
from hashlib import md5
from os.path import join

from alphabet_detector import AlphabetDetector
from bs4 import BeautifulSoup

import Config
import LogUtils
from ThirdParty.WikiCleaner import clean, dropNested


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


def get_infoboxes_filenames(directory):
    return sorted([filename for filename in os.listdir(directory) if is_infobox_file(filename)])


def is_revision_ids_file(filename):
    return True if 'revision_ids' in filename else False


def get_revision_ids_filenames(directory):
    return sorted([filename for filename in os.listdir(directory) if is_revision_ids_file(filename)])


def get_infoboxes_filename(prefix):
    return prefix+'-infoboxes'


def get_revision_ids_filename(prefix):
    return prefix+'-revision_ids'


def get_wiki_texts_filename(prefix):
    return prefix+'-wiki_texts'


def get_texts_filename(prefix):
    return prefix+'-texts'


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


def copy_directory(source, destination):
    # rewrite directory if exist
    try:
        os.system("cp -r %s %s" % (source, destination))
    except OSError:
        LogUtils.logging_warning_copy_directory(source, destination)
        return False
    return True


def delete_directory(directory):
    if exists(directory):
        if os.path.islink(directory):
            os.unlink(directory)
        else:
            shutil.rmtree(directory)


def rename_file_or_directory(source, destination):
    if exists(source):
        os.rename(source, destination)


def create_symlink(source, symlink):
    if exists(symlink):
        delete_directory(symlink)
    os.symlink(source, symlink)


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


def is_url(value):
    url_match = Config.url_regex.match(value)
    image_match = any(s in value.lower() for s in Config.images_extensions)
    wiki_links_match = 'http://fa.wikipedia.org/wiki/' in value
    if not(str(url_match) == 'None') and not image_match and not wiki_links_match:
        return True

    return False


def is_image(value):
    return any(value.lower().endswith(s) for s in Config.images_extensions)


def is_tif_image(value):
    return any(value.lower().endswith(s) for s in ['.tif', '.tiff'])


def contains_digits(d):
    return bool(Config.digits_pattern.search(d))


def line_to_list(directory, filename):
    input_filename = join(directory, filename)
    list_of_lines = list()
    with open(input_filename) as input_file:
        for line in input_file:
            list_of_lines.append(line.strip())
    return list_of_lines


def pre_clean(text):
    for template_regex in Config.expand_template_regexes:
        text = re.sub(template_regex, r'\1', text)
    text = text.replace('{{سخ}}', '</n>').replace('{{-}}', '</n>').replace('{{•}}', '</n>').replace('{{,}}', '</n>')
    text = text.replace('{{ـ}}', '').replace('{{·}}', '</n>').replace('<br>', '</n>').replace('<BR>', '</n>')
    text = text.replace('*', '</n>').replace('{{بر}}', '</n>').replace('{{سرخط}}', '</n>').replace('{{•w}}', '</n>')
    text = text.replace('{{•بشکن}}', '</n>')
    text = text.strip('\n\t -_,')
    return text


def post_clean(text, remove_newline=False):
    text = text.replace('</n>', '\n').replace('"', '').replace('()', '').replace('→', '').strip('\n\t -_,')
    text = re.sub(r"={2,}", '', text).strip()
    if not remove_newline:
        text = re.sub(r"\n+", '\n', text)
    else:
        text = re.sub(r"\n+", '', text)
    return text


def split_infobox_values(values):
    splitted_values = list()
    param_values = post_clean(values)
    param_values = dropNested(param_values, r'{{', r'}}')
    param_values = param_values.split('\n')
    for param_value in param_values:
        param_value = clean(param_value)
        only_wiki_links = re.findall(r"http://fa.wikipedia.org/wiki/\S+", param_value)
        without_wiki_links = re.sub(r"http://fa.wikipedia.org/wiki/\S+", '', param_value)
        splitters = set(' ()\\,،./-و•؟?%')
        if set(without_wiki_links) <= splitters:
            for value in only_wiki_links:
                if value:
                    splitted_values.append(post_clean(value, remove_newline=True))
        else:
            param_value = re.sub(r"http://fa.wikipedia.org/wiki/(\S+) ?", r'\1 ', param_value).replace('_', ' ').strip()
            param_value = re.sub(r'\s+', ' ', param_value)
            splitted_values.append(post_clean(param_value, remove_newline=True))

    return splitted_values


def clean_image_value(value, image_names_types_in_fawiki):
    value = re.sub(r"http://fa.wikipedia.org/wiki/(\S+) ?", r'\1', value) \
        .replace('File:', '').replace('پرونده:', '').replace(' ', '_')
    image_server = 'fa' if value in image_names_types_in_fawiki else 'commons'
    value_md5sum = md5(value.encode('utf8')).hexdigest()
    if is_tif_image(value):
        image_server += '/thumb'
        value = value + '/1000px-' + value + '.jpg'
    value = 'http://upload.wikimedia.org/wikipedia/' + image_server + '/' \
            + value_md5sum[0] + '/' + value_md5sum[:2] + '/' + value
    return value