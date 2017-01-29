import gc
import json
import logging
import os
from os.path import join, isfile

import wikitextparser as wtp

import Config
import Utils
from ThirdParty.WikiCleaner import clean
from Utils import logging_file_operations, logging_pages_extraction, get_wikipedia_pages, parse_page, \
    logging_information_extraction

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s ', level=logging.DEBUG)


def extract_wikipedia_bz2_dump(input_filename, output_dir):
    Utils.create_directory(output_dir)
    if not os.listdir(output_dir):
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


def extract_infoboxes_abstracts(filename):
    infoboxes_filename = Utils.get_information_filename(Config.extracted_infoboxes_dir, filename)
    abstracts_filename = Utils.get_information_filename(Config.extracted_abstracts_dir, filename)
    if any([isfile(infoboxes_filename), isfile(abstracts_filename)]):
        if isfile(infoboxes_filename):
            logging.info(infoboxes_filename + ' exist!')
        if isfile(abstracts_filename):
            logging.info(abstracts_filename + ' exist!')
        return

    infoboxes_dict = dict()
    abstracts_dict = dict()

    pages_counter = 0
    input_filename = join(Config.extracted_pages_articles_dir, filename)
    for page in get_wikipedia_pages(filename=input_filename):
        parsed_page = parse_page(page)
        pages_counter += 1

        if pages_counter % Config.logging_interval == 0:
            logging_information_extraction(pages_counter, input_filename)
            gc.collect()

        if parsed_page.ns.text != '0':
            continue
        text = parsed_page.revision.find('text').text
        page_name = 'kbr:' + parsed_page.title.text.replace(' ', '_')
        wiki_text = wtp.parse(text)

        templates = wiki_text.templates
        for template in templates:
            template_name = clean(str(template.name))
            if any(name in template_name for name in Config.infobox_flags):
                infobox_name = template_name.strip()
                for name in Config.infobox_flags:
                    template_name = template_name.replace(name, '')
                if template_name:
                    if page_name not in infoboxes_dict:
                        infoboxes_dict[page_name] = dict()
                    if infobox_name not in infoboxes_dict[page_name]:
                        infoboxes_dict[page_name][infobox_name] = dict()
                    for param in template.arguments:
                        param_name = clean(str(param.name))
                        param_value = clean(str(param.value))
                        if param_value:
                            infoboxes_dict[page_name][infobox_name][param_name] = param_value

        first_section = wiki_text.sections[0]
        abstract = first_section.string

        if any(name in abstract for name in Config.redirect_flags):
            continue

        templates = first_section.templates
        for template in templates:
            abstract.replace(template.string, '')

        abstract = clean(abstract, specify_wikilinks=False)
        if abstract:
            abstracts_dict[page_name] = abstract.replace('()', '')

        del templates
        del wiki_text

    logging_information_extraction(pages_counter, input_filename)
    infoboxes_json = json.dumps(infoboxes_dict, ensure_ascii=False, indent=2, sort_keys=True)
    infoboxes_file = open(infoboxes_filename, 'w+', encoding='utf8')
    logging_file_operations(infoboxes_filename, 'Opened')
    infoboxes_file.write(infoboxes_json)
    infoboxes_file.close()
    logging_file_operations(infoboxes_filename, 'Closed')

    logging_information_extraction(pages_counter, input_filename)
    abstracts_json = json.dumps(abstracts_dict, ensure_ascii=False, indent=2, sort_keys=True)
    abstracts_file = open(abstracts_filename, 'w+', encoding='utf8')
    logging_file_operations(abstracts_filename, 'Opened')
    abstracts_file.write(abstracts_json)
    abstracts_file.close()
    logging_file_operations(abstracts_filename, 'Closed')
