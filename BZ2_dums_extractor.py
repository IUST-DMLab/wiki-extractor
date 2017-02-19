import gc
import json
import logging
import os
import re
from os.path import join

import wikitextparser as wtp
from joblib import Parallel, delayed

import Config
import Utils
from ThirdParty.WikiCleaner import clean
from Utils import logging_file_operations, logging_pages_extraction, get_wikipedia_pages, parse_page, \
    logging_information_extraction

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s ', level=logging.DEBUG)


# leila 1.12.95
def get_template_name_type(template_name):

    template_name = clean(str(template_name).lower().replace('الگو:', ' '))
    lang = Utils.detect_language(template_name)

    if lang == 'fa':

        if any(s in template_name for s in Config.infobox_flags_fa):
            infobox_name = template_name
            template_type = 'infobox'

            return infobox_name, template_type, lang
        elif any(s in template_name for s in Config.stub_flag_fa):
            return template_name, 'stub', lang

        else:
            return template_name, 'template', lang
    else:
        template_name = clean(str(template_name).lower().replace('_', ' '))

        if any(s in template_name for s in Config.infobox_flags_en):

            infobox_name = template_name
            template_type = 'infobox'

            return infobox_name, template_type, lang
        elif any(s in template_name for s in Config.stub_flag_en):

            stub_name = template_name
            template_type = 'stub'

            return stub_name, template_type, lang

        else:
            return template_name, 'template', lang

# end


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


def extract_bz2_dump(filename):

    infoboxes = dict()
    page_ids = dict()
    revision_ids = dict()
    wiki_texts = dict()
    abstracts = dict()

    with_infobox_page_path = dict()
    without_infobox_list = set()

    # leila 1.12.95 --->start
    list_template = []
    # end

    pages_counter = 0
    input_filename = join(Config.extracted_pages_articles_dir, filename)
    for page in get_wikipedia_pages(filename=input_filename):
        parsed_page = parse_page(page)
        pages_counter += 1

        if pages_counter % Config.logging_interval == 0:
            logging_information_extraction(pages_counter, input_filename)
            gc.collect()

        # leila 1.12.95
        if parsed_page.ns.text == '10':
            dict_template = {}
            template_name, template_type, lang = get_template_name_type(parsed_page.title.text)

            dict_template['template_name'] = template_name
            dict_template['type'] = template_type
            dict_template['language_name'] = lang
            list_template.append(dict_template)
        # end

        if parsed_page.ns.text != '0':
            continue

        page_name = parsed_page.title.text
        page_id = parsed_page.id.text
        revision_id = parsed_page.revision.id.text
        text = parsed_page.revision.find('text').text

        page_ids[page_id] = page_name
        revision_ids[page_name] = revision_id
        wiki_texts[page_name] = text

        wiki_text = wtp.parse(text)

        first_section = wiki_text.sections[0]
        abstract = first_section.string

        if not any(name in abstract for name in Config.redirect_flags)\
                and not any(name in page_name for name in Config.disambigution_flags):
            first_section_templates = first_section.templates
            for template in first_section_templates:
                infobox_name, infobox_type = Utils.find_get_infobox_name_type(template.name)
                if infobox_name:
                    abstract = abstract.replace(template.string, '')

            abstract = clean(abstract, specify_wikilinks=False).replace('()', '')

            abstracts[page_name] = abstract

        templates = wiki_text.templates
        for template in templates:
            infobox_name, infobox_type = Utils.find_get_infobox_name_type(template.name)
            if infobox_name and infobox_type:
                if infobox_name not in infoboxes:
                    infoboxes[infobox_name] = dict()

                if infobox_type not in infoboxes[infobox_name]:
                    infoboxes[infobox_name][infobox_type] = dict()

                if page_name not in infoboxes[infobox_name][infobox_type]:
                    infoboxes[infobox_name][infobox_type][page_name] = list()

                infobox = dict()
                for param in template.arguments:
                    param_name = clean(str(param.name))
                    param_value = clean(str(param.value))
                    if param_value:
                        param_value = re.split(r'\\\\|,|،', param_value)
                        param_value = [value.strip() for value in param_value]
                        infobox[param_name] = param_value

                infoboxes[infobox_name][infobox_type][page_name].append(infobox)

            else:
                without_infobox_list.add(page_name)

        del templates
        del wiki_text

    for infobox_name in infoboxes:
        for infobox_type in infoboxes[infobox_name]:
            path = join(infobox_name, infobox_type)
            if path not in with_infobox_page_path:
                with_infobox_page_path[path] = list()

            for key in infoboxes[infobox_name][infobox_type]:
                with_infobox_page_path[path].append(key)

            absolute_resource_path = join(Config.extracted_pages_with_infobox_dir, infobox_name, infobox_type)

            Utils.save_json(absolute_resource_path, Utils.get_infoboxes_filename(filename),
                            infoboxes[infobox_name][infobox_type])

            Utils.save_json(absolute_resource_path, Utils.get_revision_ids_filename(filename),
                            revision_ids, filter_dict=infoboxes[infobox_name][infobox_type])

            Utils.save_json(absolute_resource_path, Utils.get_wiki_texts_filename(filename),
                            wiki_texts, filter_dict=infoboxes[infobox_name][infobox_type])

            Utils.save_json(absolute_resource_path, Utils.get_abstracts_filename(filename),
                            abstracts, filter_dict=infoboxes[infobox_name][infobox_type])

    Utils.save_json(Config.extracted_pages_path_with_infobox_dir, filename, with_infobox_page_path)

    Utils.save_json(Config.extracted_pages_path_without_infobox_dir, filename, list(without_infobox_list))

    Utils.save_json(Config.extracted_page_ids_dir, filename, page_ids)

    Utils.save_json(Config.extracted_pages_without_infobox_dir, Utils.get_revision_ids_filename(filename),
                    revision_ids, filter_dict=without_infobox_list)

    Utils.save_json(Config.extracted_pages_without_infobox_dir, Utils.get_wiki_texts_filename(filename),
                    wiki_texts, filter_dict=without_infobox_list)

    Utils.save_json(Config.extracted_pages_without_infobox_dir, Utils.get_abstracts_filename(filename),
                    abstracts, filter_dict=without_infobox_list)

    # leila 1.12.95 --->start
    Utils.save_json(Config.extracted_template_name_dir, 'wiki_farsi_templates', list_template)
    # end

    Utils.create_directory(Config.extracted_infoboxes_dir)
    with open(Utils.get_information_filename(Config.extracted_infoboxes_dir, filename), 'w+', encoding='utf8') as fp:
        fp.write('[\n')
        for infobox_name in infoboxes:
            for infobox_type in infoboxes[infobox_name]:
                for page_name in infoboxes[infobox_name][infobox_type]:
                    for infobox in infoboxes[infobox_name][infobox_type][page_name]:
                        for predicate in infobox:
                            for value in infobox[predicate]:
                                json_dict = dict()
                                json_dict['template_name'] = infobox_name
                                json_dict['type'] = infobox_type if infobox_type != 'NULL' else None
                                json_dict['subject'] = 'fa.wikipedia.org/wiki/'+page_name.replace(' ', '_')
                                json_dict['predicate'] = predicate
                                json_dict['object'] = value
                                json_dict['source'] = 'fa.wikipedia.org/wiki/'+page_name.replace(' ', '_')
                                json_dict['version'] = revision_ids[page_name]
                                json.dump(json_dict, fp, ensure_ascii=False, indent=2, sort_keys=True)
                                fp.write('\n,\n')
        fp.write(']\n')

    logging_information_extraction(pages_counter, input_filename)


def extract_all():
    extract_wikipedia_bz2_dump(Config.fawiki_latest_pages_articles_dump, Config.extracted_pages_articles_dir)
    extract_wikipedia_bz2_dump(Config.fawiki_latest_pages_meta_current_dump, Config.extracted_pages_meta_current_dir)

    extracted_pages_files = os.listdir(Config.extracted_pages_articles_dir)
    if extracted_pages_files:
        Parallel(n_jobs=4)(delayed(extract_bz2_dump)(filename) for filename in extracted_pages_files)
