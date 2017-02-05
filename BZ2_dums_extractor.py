import gc
import json
import logging
import os
from os.path import join

import wikitextparser as wtp

import Config
import Utils
from ThirdParty.WikiCleaner import clean
from Utils import logging_file_operations, logging_pages_extraction, get_wikipedia_pages, parse_page, \
    logging_information_extraction

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s ', level=logging.DEBUG)


def extract_wikipedia_bz2_dump(input_filename, output_dir):
    Utils.create_directory(output_dir, show_logging=True)
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
    pages_path_dict = dict()
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
        page_name = parsed_page.title.text
        if page_name not in pages_path_dict:
            pages_path_dict[page_name] = list()
        wiki_text = wtp.parse(text)

        has_infobox = False
        templates = wiki_text.templates
        for template in templates:
            infobox_name, infobox_type = Utils.find_get_infobox_name_type(template.name)
            if infobox_name and infobox_type:
                has_infobox = True
                if page_name not in infoboxes_dict:
                    infoboxes_dict[page_name] = dict()
                if infobox_name not in infoboxes_dict[page_name]:
                    infoboxes_dict[page_name][infobox_name] = dict()
                if infobox_type not in infoboxes_dict[page_name][infobox_name]:
                    infoboxes_dict[page_name][infobox_name][infobox_type] = dict()

                for param in template.arguments:
                    param_name = clean(str(param.name))
                    param_value = clean(str(param.value))
                    if param_value:
                        infoboxes_dict[page_name][infobox_name][infobox_type][param_name] = param_value

                relative_resource_path = join(infobox_name, infobox_type, page_name.replace('/', '∕'))
                if len(relative_resource_path) < 255:
                    absolute_resource_path = join(Config.extracted_pages_with_infobox_dir,
                                                  relative_resource_path)
                    pages_path_dict[page_name].append(absolute_resource_path)

                    Utils.create_directory(absolute_resource_path)
                    infobox_filename = Utils.get_information_filename(absolute_resource_path, 'infobox')
                    infobox_file = open(infobox_filename, 'w+', encoding='utf8')
                    infobox_json = json.dumps(infoboxes_dict[page_name][infobox_name][infobox_type],
                                              ensure_ascii=False, indent=2, sort_keys=True)
                    infobox_file.write(infobox_json)
                    infobox_file.close()

        relative_resource_path = join(page_name[0].replace('/', '∕'), page_name[:2].replace('/', '∕'),
                                      page_name.replace('/', '∕'))
        if not has_infobox and len(relative_resource_path) < 255:
            absolute_resource_path = join(Config.extracted_pages_without_infobox_dir, relative_resource_path)
            pages_path_dict[page_name].append(absolute_resource_path)

        first_section = wiki_text.sections[0]
        abstract = first_section.string

        if not any(name in abstract for name in Config.redirect_flags)\
                and not any(name in abstract for name in Config.disambigution_flags)\
                and not any(name in page_name for name in Config.disambigution_flags):
            first_section_templates = first_section.templates
            for template in first_section_templates:
                infobox_name, infobox_type = Utils.find_get_infobox_name_type(template.name)
                if infobox_name:
                    abstract = abstract.replace(template.string, '')

            abstract = clean(abstract, specify_wikilinks=False).replace('()', '')
            abstracts_dict[page_name] = abstract

            for path in pages_path_dict[page_name]:
                absolute_resource_path = path
                if page_name in abstracts_dict:
                    Utils.create_directory(absolute_resource_path)
                    abstract_filename = join(absolute_resource_path, 'abstract.txt')
                    abstract_file = open(abstract_filename, 'w+', encoding='utf8')
                    abstract_file.write(abstracts_dict[page_name])
                    abstract_file.close()

        del templates
        del wiki_text

    logging_information_extraction(pages_counter, input_filename)

    Utils.create_directory(Config.extracted_pages_path_dir, show_logging=True)
    pages_path_filename = Utils.get_information_filename(Config.extracted_pages_path_dir, filename)
    pages_path_file = open(pages_path_filename, 'w+', encoding='utf8')
    pages_path_json = json.dumps(pages_path_dict, ensure_ascii=False, indent=2, sort_keys=True)
    pages_path_file.write(pages_path_json)
    pages_path_file.close()
