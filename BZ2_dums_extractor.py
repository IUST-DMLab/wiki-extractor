import gc
import logging
import os
import re
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
    with_infoboxes_dict = dict()
    without_infoboxes_dict = dict()

    infoboxes_flag = Config.information_flags['infoboxes_flag']
    abstracts_flag = Config.information_flags['abstracts_flag']
    revision_id_flag = Config.information_flags['revision_id_flag']
    wiki_text_flag = Config.information_flags['wiki_text_flag']

    without_infoboxes_dict[abstracts_flag] = dict()
    without_infoboxes_dict[revision_id_flag] = dict()
    without_infoboxes_dict[wiki_text_flag] = dict()

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
        revision_id = parsed_page.revision.id.text
        pages_path_dict[page_name] = list()
        wiki_text = wtp.parse(text)

        has_infobox = False
        templates = wiki_text.templates
        for template in templates:
            infobox_name, infobox_type = Utils.find_get_infobox_name_type(template.name)
            if infobox_name and infobox_type:
                has_infobox = True
                if infobox_name not in with_infoboxes_dict:
                    with_infoboxes_dict[infobox_name] = dict()

                if infobox_type not in with_infoboxes_dict[infobox_name]:
                    with_infoboxes_dict[infobox_name][infobox_type] = dict()

                for flag in Config.information_flags:
                    information_flag = Config.information_flags[flag]
                    if information_flag not in with_infoboxes_dict[infobox_name][infobox_type]:
                        with_infoboxes_dict[infobox_name][infobox_type][information_flag] = dict()

                if page_name not in with_infoboxes_dict[infobox_name][infobox_type][infoboxes_flag]:
                    with_infoboxes_dict[infobox_name][infobox_type][infoboxes_flag][page_name] = list()

                infobox_dict = dict()
                for param in template.arguments:
                    param_name = clean(str(param.name))
                    param_value = clean(str(param.value))
                    if param_value:
                        param_value = re.split(r'\\\\|/|,|،', param_value)
                        param_value = [value.strip() for value in param_value]
                        infobox_dict[param_name] = param_value

                with_infoboxes_dict[infobox_name][infobox_type][infoboxes_flag][page_name].append(infobox_dict)

                if len(with_infoboxes_dict[infobox_name][infobox_type][infoboxes_flag][page_name]) == 1:
                    pages_path_dict[page_name].append([infobox_name, infobox_type])

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
            if has_infobox:
                for path in pages_path_dict[page_name]:
                    infobox_name = path[0]
                    infobox_type = path[1]
                    with_infoboxes_dict[infobox_name][infobox_type][abstracts_flag][page_name] = abstract
                    with_infoboxes_dict[infobox_name][infobox_type][revision_id_flag][page_name] = revision_id
                    with_infoboxes_dict[infobox_name][infobox_type][wiki_text_flag][page_name] = text
            else:
                without_infoboxes_dict[abstracts_flag][page_name] = abstract
                without_infoboxes_dict[revision_id_flag][page_name] = revision_id
                without_infoboxes_dict[wiki_text_flag][page_name] = text

        del templates
        del wiki_text

    for infobox_name in with_infoboxes_dict:
        for infobox_type in with_infoboxes_dict[infobox_name]:
            absolute_resource_path = join(Config.extracted_pages_with_infobox_dir, infobox_name, infobox_type)
            if len(absolute_resource_path) < 255:
                Utils.create_directory(absolute_resource_path)

                for flag in Config.information_flags:
                    information_flag = Config.information_flags[flag]
                    information_filename = Utils.get_information_filename(absolute_resource_path,
                                                                          filename+'-'+information_flag)
                    Utils.save_dict_to_json_file(information_filename,
                                                 with_infoboxes_dict[infobox_name][infobox_type][information_flag])

    absolute_resource_path = Config.extracted_pages_without_infobox_dir
    if len(absolute_resource_path) < 255:
        Utils.create_directory(absolute_resource_path)
        for flag in Config.information_flags:
            information_flag = Config.information_flags[flag]
            if information_flag != infoboxes_flag:
                filename = Utils.get_information_filename(absolute_resource_path, filename + '-' + information_flag)
                Utils.save_dict_to_json_file(filename, without_infoboxes_dict[information_flag])

    logging_information_extraction(pages_counter, input_filename)
