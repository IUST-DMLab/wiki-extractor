import bz2
import gc
import json
import logging
import os
from os.path import join, exists

import wikitextparser as wtp
from bs4 import BeautifulSoup

import Config
from ThirdParty.WikiCleaner import clean

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s ', level=logging.DEBUG)


def logging_file_operations(filename, operation):
    logging.info('%s %s!' % (filename, operation))


def logging_pages_extraction(pages_number, filename):
    logging.info('%d Pages Extracted from %s!' % (pages_number, filename))


def logging_information_extraction(pages_number, filename, info):
    logging.info('%d Pages Checked from %s for %s!' % (pages_number, filename, info))


def get_information_filename(info_dir, file_number):
    return join(info_dir, str(file_number)+'.json')


def create_directory(directory):
    if not exists(directory):
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


def extract_wikipedia_pages(input_filename, output_dir):
    pages_counter = 0
    file_number = int(pages_counter/Config.extracted_pages_per_file)
    extracted_filename = join(output_dir, str(file_number))
    extracted_pages_file = open(extracted_filename, 'w+')
    logging_file_operations(extracted_filename, 'Opened')

    for page in get_wikipedia_pages(input_filename):
        extracted_pages_file.write(page)
        pages_counter += 1
        if pages_counter % Config.extracted_pages_per_file == 0:
            logging_pages_extraction(pages_counter, extracted_filename)
            extracted_pages_file.close()
            logging_file_operations(extracted_filename, 'Closed')
            file_number = int(pages_counter/Config.extracted_pages_per_file)
            extracted_filename = join(output_dir, str(file_number))
            extracted_pages_file = open(extracted_filename, 'w+')
            logging_file_operations(extracted_filename, 'Opened')

    logging_pages_extraction(pages_counter, extracted_filename)
    extracted_pages_file.close()
    logging_file_operations(extracted_filename, 'Closed')
    logging.info('Page Extraction Finished! Number of All Extracted Pages: %d' % pages_counter)


def parse_page(xml_page):
    soup = BeautifulSoup(xml_page, "xml")
    page = soup.find('page')
    return page


def extract_infoboxes(input_filename, output_filename):
    infoboxes_dict_clean = dict()
    infoboxes_dict_unclean = dict()
    pages_counter = 0
    for page in get_wikipedia_pages(filename=input_filename):
        parsed_page = parse_page(page)
        pages_counter += 1

        if pages_counter % Config.logging_interval == 0:
            logging_information_extraction(pages_counter, input_filename, 'infoboxes')
            gc.collect()

        if parsed_page.ns.text != '0':
            continue
        text = parsed_page.revision.find('text').text
        wiki_text = wtp.parse(text)
        templates = wiki_text.templates
        for template in templates:
            template_name = clean(str(template.name))
            if any(name in template_name for name in Config.infobox_flags) or 'box' in template_name:
                page_name = 'kbr:' + parsed_page.title.text.replace(' ', '_')
                infobox_name = template_name
                for name in Config.infobox_flags:
                    infobox_name = infobox_name.replace(name, '')
                infobox_name = infobox_name.strip()
                if infobox_name:
                    if page_name not in infoboxes_dict_clean:
                        infoboxes_dict_clean[page_name] = dict()
                        infoboxes_dict_unclean[page_name] = dict()
                    if infobox_name not in infoboxes_dict_clean[page_name]:
                        infoboxes_dict_clean[page_name][infobox_name] = dict()
                        infoboxes_dict_unclean[page_name][infobox_name] = dict()
                    for param in template.arguments:
                        param_name = clean(str(param.name))
                        param_value = clean(str(param.value))
                        if param_value:
                            infoboxes_dict_clean[page_name][infobox_name][param_name] = param_value
                            infoboxes_dict_unclean[page_name][infobox_name][param_name] = str(param.value)
        del templates
        del wiki_text

    logging_information_extraction(pages_counter, input_filename, 'infoboxes')
    infoboxes_json_clean = json.dumps(infoboxes_dict_clean, ensure_ascii=False, indent=2, sort_keys=True)
    infoboxes_file_clean = open(output_filename, 'w+', encoding='utf8')
    logging_file_operations(output_filename, 'Opened')
    infoboxes_file_clean.write(infoboxes_json_clean)
    infoboxes_file_clean.close()
    logging_file_operations(output_filename, 'Closed')

    infoboxes_json_unclean = json.dumps(infoboxes_dict_unclean, ensure_ascii=False, indent=2, sort_keys=True)
    infoboxes_file_unclean = open(output_filename + '.unclean', 'w+', encoding='utf8')
    infoboxes_file_unclean.write(infoboxes_json_unclean)
    infoboxes_file_unclean.close()


def extract_ids(input_filename, output_filename):
    ids_dict = dict()
    pages_counter = 0
    for page in get_wikipedia_pages(filename=input_filename):
        parsed_page = parse_page(page)
        pages_counter += 1
        page_name = 'kbr:' + parsed_page.title.text.replace(' ', '_')
        page_id = parsed_page.id.text
        ids_dict[page_id] = page_name
        if pages_counter % Config.logging_interval == 0:
            logging_information_extraction(pages_counter, input_filename, 'ids')
            gc.collect()

    logging_information_extraction(pages_counter, input_filename, 'ids')
    ids_json = json.dumps(ids_dict, ensure_ascii=False, indent=2, sort_keys=True)
    ids_file = open(output_filename, 'w+', encoding='utf8')
    logging_file_operations(output_filename, 'Opened')
    ids_file.write(ids_json)
    ids_file.close()
    logging_file_operations(output_filename, 'Closed')


def extract_abstracts(input_filename, output_filename):
    abstracts_dict_clean = dict()
    abstracts_dict_unclean = dict()

    pages_counter = 0
    for page in get_wikipedia_pages(filename=input_filename):
        parsed_page = parse_page(page)
        pages_counter += 1

        if pages_counter % Config.logging_interval == 0:
            logging_information_extraction(pages_counter, input_filename, 'abstracts')
            gc.collect()

        if parsed_page.ns.text != '0':
            continue

        text = parsed_page.revision.find('text').text
        wiki_text = wtp.parse(text)
        first_section = wiki_text.sections[0]
        abstract = first_section.string
        if 'تغییر_مسیر' in abstract or 'تغییرمسیر' in abstract or 'REDIRECT' in abstract:
            continue
        templates = first_section.templates
        for template in templates:
            template_name = clean(str(template.name))
            if 'Infobox' in template_name or 'جعبه اطلاعات' in template_name or 'جعبه' in template_name:
                abstract.replace(template.string, '')
        abstract = clean(abstract, specify_wikilinks=False)
        if abstract:
            page_name = 'kbr:' + parsed_page.title.text.replace(' ', '_')
            abstracts_dict_clean[page_name] = abstract
            abstracts_dict_unclean[page_name] = str(first_section)

        del wiki_text
        del first_section

    logging_information_extraction(pages_counter, input_filename, 'abstracts')
    abstracts_json_clean = json.dumps(abstracts_dict_clean, ensure_ascii=False, indent=2, sort_keys=True)
    abstracts_file_clean = open(output_filename, 'w+', encoding='utf8')
    logging_file_operations(output_filename, 'Opened')
    abstracts_file_clean.write(abstracts_json_clean)
    abstracts_file_clean.close()
    logging_file_operations(output_filename, 'Closed')

    abstracts_json_unclean = json.dumps(abstracts_dict_unclean, ensure_ascii=False, indent=2, sort_keys=True)
    abstracts_file_unclean = open(output_filename + '.unclean', 'w+', encoding='utf8')
    abstracts_file_unclean.write(abstracts_json_unclean)
    abstracts_file_unclean.close()


def logging_database(msg):
    logging.exception('%s Error!' % msg)
