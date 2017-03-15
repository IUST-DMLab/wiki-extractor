import copy
import gc
import logging
import os
import re
from collections import defaultdict
from os.path import join

import wikitextparser as wtp
from joblib import Parallel, delayed

import Config
import Utils
from ThirdParty.WikiCleaner import clean


def extract_bz2_dump(input_filename, output_dir):
    Utils.create_directory(output_dir, show_logging=True)
    if not os.listdir(output_dir):
        pages_counter = 0
        extracted_pages_filename, extracted_pages_file = Utils.open_extracted_bz2_dump_file(pages_counter, output_dir)

        for page in Utils.get_wikipedia_pages(input_filename):
            extracted_pages_file.write(page)
            pages_counter += 1
            if pages_counter % Config.extracted_pages_per_file == 0:
                Utils.logging_pages_extraction(pages_counter, extracted_pages_filename)
                Utils.close_extracted_bz2_dump_file(extracted_pages_filename, extracted_pages_file)
                extracted_pages_filename, extracted_pages_file = Utils.open_extracted_bz2_dump_file(pages_counter,
                                                                                                    output_dir)

        Utils.logging_pages_extraction(pages_counter, extracted_pages_filename)
        Utils.close_extracted_bz2_dump_file(extracted_pages_filename, extracted_pages_file)
        logging.info('Page Extraction Finished! Number of All Extracted Pages: %d' % pages_counter)


def extract_bz2_dump_information(directory, filename,
                                 extract_abstracts=False,
                                 extract_page_ids=False,
                                 extract_revision_ids=False,
                                 extract_wiki_texts=False,
                                 extract_pages_infoboxes=False,
                                 extract_template_names=False,
                                 extracted_template_names_dir=None):
    abstracts = dict()
    page_ids = dict()
    revision_ids = dict()
    wiki_texts = dict()
    infoboxes = dict()
    pages_with_infobox = dict()
    pages_without_infobox = list()
    template_names_list = list()

    pages_counter = 0
    input_filename = join(directory, filename)
    for page in Utils.get_wikipedia_pages(filename=input_filename):
        parsed_page = Utils.parse_page(page)
        pages_counter += 1

        if pages_counter % Config.logging_interval == 0:
            Utils.logging_information_extraction(pages_counter, input_filename)
            gc.collect()

        if extract_template_names and parsed_page.ns.text == '10':
            template_dict = dict()
            template_name, template_type, lang = Utils.get_template_name_type(parsed_page.title.text)

            template_dict['template_name'] = template_name
            template_dict['type'] = template_type
            template_dict['language_name'] = lang
            template_names_list.append(template_dict)

        if parsed_page.ns.text != '0':
            continue

        page_name = parsed_page.title.text
        page_id = parsed_page.id.text
        revision_id = parsed_page.revision.id.text
        text = parsed_page.revision.find('text').text

        if extract_page_ids:
            page_ids[page_id] = page_name
        if extract_revision_ids:
            revision_ids[page_name] = revision_id
        if extract_wiki_texts:
            wiki_texts[page_name] = text

        wiki_text = str()
        if extract_abstracts or extract_pages_infoboxes:
            wiki_text = wtp.parse(text)

        if extract_abstracts:
            first_section = wiki_text.sections[0]
            abstract = first_section.string

            if not any(name in abstract for name in Config.redirect_flags)\
                    and not any(name in page_name for name in Config.disambigution_flags):
                first_section_templates = first_section.templates
                for template in first_section_templates:
                    abstract = abstract.replace(template.string, '').replace('()', '')

                abstract = clean(abstract, specify_wikilinks=False)
                abstracts[page_name] = abstract

        if extract_pages_infoboxes:
            page_has_infobox = False
            template_names = wiki_text.templates
            for template in template_names:
                template_name, infobox_type = Utils.get_infobox_name_type(template.name)
                if infobox_type:
                    page_has_infobox = True
                    if template_name not in infoboxes:
                        infoboxes[template_name] = dict()

                    if page_name not in infoboxes[template_name]:
                        infoboxes[template_name][page_name] = list()

                    if page_name not in pages_with_infobox:
                        pages_with_infobox[page_name] = list()

                    if template_name not in pages_with_infobox[page_name]:
                        pages_with_infobox[page_name].append(template_name)

                    infobox = dict()
                    for param in template.arguments:
                        param_name = clean(str(param.name))
                        param_value = str(param.value).replace('{{سخ}}', '،').replace('{{-}}', '،')
                        param_value = param_value.replace('<br>', '،').replace('*', '،')
                        param_value = clean(param_value)
                        param_value = re.sub(r"\s+", ' ', param_value)
                        param_value = param_value.replace(' ، ', '،')
                        only_wiki_links = re.findall(r"http://fa.wikipedia.org/wiki/\S+", param_value)
                        if ' '.join(only_wiki_links) == param_value:
                            param_value = param_value.replace(' ', '،')
                        if ' و '.join(only_wiki_links) == param_value:
                            param_value = param_value.replace(' و ', '،')
                        if ' - '.join(only_wiki_links) == param_value:
                            param_value = param_value.replace(' - ', '،')
                        if ' / '.join(only_wiki_links) == param_value:
                            param_value = param_value.replace(' / ', '،')
                        if param_value:
                            param_value = re.split(r'\\\\|,|،', param_value)
                            param_value = [value.strip() for value in param_value]
                            infobox[param_name] = param_value

                    infoboxes[template_name][page_name].append(infobox)

            if not page_has_infobox:
                pages_without_infobox.append(page_name)

            del template_names
        del wiki_text

    if extract_abstracts:
        Utils.save_json(Config.extracted_abstracts_dir, filename, abstracts)
        if extract_pages_infoboxes:
            Utils.save_json(Config.extracted_with_infobox_dir, Utils.get_abstracts_filename(filename),
                            abstracts, filter_dict=pages_with_infobox)
            Utils.save_json(Config.extracted_without_infobox_dir, Utils.get_abstracts_filename(filename),
                            abstracts, filter_dict=pages_without_infobox)

    if extract_page_ids:
        Utils.save_json(Config.extracted_page_ids_dir, filename, page_ids)

    if extract_revision_ids:
        Utils.save_json(Config.extracted_revision_ids_dir, filename, revision_ids)
        if extract_pages_infoboxes:
            Utils.save_json(Config.extracted_with_infobox_dir, Utils.get_revision_ids_filename(filename),
                            revision_ids, filter_dict=pages_with_infobox)
            Utils.save_json(Config.extracted_without_infobox_dir, Utils.get_revision_ids_filename(filename),
                            revision_ids, filter_dict=pages_without_infobox)

    if extract_wiki_texts:
        Utils.save_json(Config.extracted_wiki_texts_dir, filename, wiki_texts)
        if extract_pages_infoboxes:
            Utils.save_json(Config.extracted_with_infobox_dir, Utils.get_wiki_texts_filename(filename),
                            wiki_texts, filter_dict=pages_with_infobox)
            Utils.save_json(Config.extracted_without_infobox_dir, Utils.get_wiki_texts_filename(filename),
                            wiki_texts, filter_dict=pages_without_infobox)

    if extract_pages_infoboxes:
        Utils.save_json(Config.extracted_with_infobox_dir, Utils.get_infoboxes_filename(filename), infoboxes)
        Utils.save_json(Config.extracted_pages_with_infobox_dir, filename, pages_with_infobox)
        Utils.save_json(Config.extracted_pages_without_infobox_dir, filename, pages_without_infobox)

    if extract_template_names:
        Utils.save_json(extracted_template_names_dir, filename, template_names_list)

    Utils.logging_information_extraction(pages_counter, input_filename)


def extract_fawiki_latest_pages_articles_dump():
    input_filename = Config.fawiki_latest_pages_articles_dump
    output_dir = Config.extracted_fa_pages_articles_dir
    extract_bz2_dump(input_filename, output_dir)


def extract_enwiki_latest_pages_articles_dump():
    input_filename = Config.enwiki_latest_pages_articles_dump
    output_dir = Config.extracted_en_pages_articles_dir
    extract_bz2_dump(input_filename, output_dir)


def multiprocess_extraction(directory, parameters):
    filenames = os.listdir(directory)
    if filenames:
        Parallel(n_jobs=-1)(delayed(extract_bz2_dump_information)(directory, filename, **parameters)
                            for filename in filenames)


def extract_fawiki_abstracts():
    directory = Config.extracted_fa_pages_articles_dir
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_abstracts'] = True
    multiprocess_extraction(directory, parameters)


def extract_fawiki_page_ids():
    directory = Config.extracted_fa_pages_articles_dir
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_page_ids'] = True
    multiprocess_extraction(directory, parameters)


def extract_fawiki_revision_ids():
    directory = Config.extracted_fa_pages_articles_dir
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_revision_ids'] = True
    multiprocess_extraction(directory, parameters)


def extract_fawiki_wiki_texts():
    directory = Config.extracted_fa_pages_articles_dir
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_wiki_texts'] = True
    multiprocess_extraction(directory, parameters)


def extract_fawiki_pages_infoboxes():
    directory = Config.extracted_fa_pages_articles_dir
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_pages_infoboxes'] = True
    multiprocess_extraction(directory, parameters)


def extract_fawiki_template_names():
    directory = Config.extracted_fa_pages_articles_dir
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_template_names'] = True
    parameters['extracted_template_names_dir'] = Config.extracted_fa_template_names_dir
    multiprocess_extraction(directory, parameters)


def extract_enwiki_template_names():
    directory = Config.extracted_en_pages_articles_dir
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_template_names'] = True
    parameters['extracted_template_names_dir'] = Config.extracted_en_template_names_dir
    multiprocess_extraction(directory, parameters)


def extract_fawiki_bz2_dump_information():
    extract_fawiki_latest_pages_articles_dump()
    directory = Config.extracted_fa_pages_articles_dir
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_abstracts'] = True
    parameters['extract_page_ids'] = True
    parameters['extract_revision_ids'] = True
    parameters['extract_wiki_texts'] = True
    parameters['extract_pages_infoboxes'] = True
    parameters['extract_template_names'] = True
    parameters['extracted_template_names_dir'] = Config.extracted_fa_template_names_dir
    multiprocess_extraction(directory, parameters)


def extract_page_ids_from_sql_dump():
    all_records = Utils.get_sql_rows(Config.fawiki_latest_page_dump)

    page_ids = dict()
    for record in all_records:
        for columns in record:
            page_id, page_namespace, page_title = columns[0], columns[1], columns[2]
            page_ids[page_id] = page_title

    Utils.save_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename, page_ids)


def extract_lang_links_from_sql_dump():
    lang_links_en = dict()
    lang_links_ar = dict()
    page_ids = Utils.load_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename)

    all_records = Utils.get_sql_rows(Config.fawiki_latest_lang_links_dump)
    for record in all_records:
        for columns in record:
            ll_from, ll_lang, ll_title = columns[0], columns[1], columns[2]
            if ll_from in page_ids:
                ll_from = page_ids[ll_from]

                if ll_lang == 'en':
                    lang_links_en[ll_from] = ll_title
                elif ll_lang == 'ar':
                    lang_links_ar[ll_from] = ll_title

    Utils.save_json(Config.extracted_lang_links_dir, Config.extracted_en_lang_link_filename, lang_links_en)
    Utils.save_json(Config.extracted_lang_links_dir, Config.extracted_ar_lang_link_filename, lang_links_ar)


def extract_redirects_from_sql_dump():
    redirects = dict()
    reverse_redirects = dict()
    page_ids = Utils.load_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename)

    all_records = Utils.get_sql_rows(Config.fawiki_latest_redirect_dump)
    for record in all_records:
        for columns in record:
            r_from, ns, r_title = columns[0], columns[1], columns[2]
            if r_from in page_ids:
                r_from = page_ids[r_from]
                if ns not in redirects:
                    redirects[ns] = dict()
                if ns not in reverse_redirects:
                    reverse_redirects[ns] = dict()
                if r_title not in reverse_redirects[ns]:
                    reverse_redirects[ns][r_title] = list()
                redirects[ns][r_from] = r_title
                reverse_redirects[ns][r_title].append(r_from)

    for ns in redirects:
        Utils.save_json(Config.extracted_redirects_dir, Utils.get_redirects_filename(ns), redirects[ns])
    for ns in reverse_redirects:
        Utils.save_json(Config.extracted_reverse_redirects_dir, Utils.get_redirects_filename(ns), reverse_redirects[ns])

    sql_dump = Utils.get_wiki_template_redirect_sql_dump(redirects['10'])
    Utils.save_sql_dump(Config.extracted_redirects_dir, '10-redirects.sql', sql_dump)


def extract_category_links_from_sql_dump(page_ids, output_directory, output_filename):
    category_links = defaultdict(list)

    all_records = Utils.get_sql_rows(Config.fawiki_latest_category_links_dump)
    for record in all_records:
        for columns in record:
            cl_from, cl_to = columns[0], columns[1]
            if cl_from in page_ids:
                cl_from = page_ids[cl_from]
                category_links[cl_from].append(cl_to)

    Utils.save_json(output_directory, output_filename, category_links)


def extract_external_links_from_sql_dump(page_ids, output_directory, output_filename):
    external_links = defaultdict(list)

    all_records = Utils.get_sql_rows(Config.fawiki_latest_external_links_dump)
    for record in all_records:
        for columns in record:
            el_from, el_to = columns[1], columns[3]
            if el_from in page_ids:
                el_from = page_ids[el_from]
                external_links[el_from].append(el_to)

    Utils.save_json(output_directory, output_filename, external_links)


def extract_wiki_links_from_sql_dump(page_ids, output_directory, output_filename):
    wiki_links = defaultdict(list)

    all_records = Utils.get_sql_rows(Config.fawiki_latest_page_links_dump)
    for record in all_records:
        for columns in record:
            pl_from, pl_title = columns[0], columns[2]
            if pl_from in page_ids:
                pl_from = page_ids[pl_from]
                wiki_links[pl_from].append(pl_title)

    Utils.save_json(output_directory, output_filename, wiki_links)


def extract_category_external_wiki_links_from_sql_dumps():
    page_ids = Utils.load_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename)

    extract_category_links_from_sql_dump(page_ids, Config.extracted_category_links_dir,
                                         Config.extracted_category_links_filename)

    extract_external_links_from_sql_dump(page_ids,
                                         Config.extracted_external_links_dir, Config.extracted_external_links_filename)

    del page_ids

    page_ids_filenames = os.listdir(Config.extracted_page_ids_dir)
    page_ids_filenames.remove(Config.extracted_page_ids_filename+'.json')

    for page_ids_filename in page_ids_filenames:
        page_ids = Utils.load_json(Config.extracted_page_ids_dir, page_ids_filename)

        extract_wiki_links_from_sql_dump(page_ids, Config.extracted_wiki_links_dir, page_ids_filename)
