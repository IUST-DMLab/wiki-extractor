import copy
import gc
import logging
import os
from collections import defaultdict
from os.path import join

import wikitextparser as wtp
from joblib import Parallel, delayed

import Config
import DataUtils
import LogUtils
import SqlUtils
from ThirdParty.WikiCleaner import clean


def extract_bz2_dump(lang):
    input_filename = Config.latest_pages_articles_dump[lang]
    output_dir = Config.extracted_pages_articles_dir[lang]
    DataUtils.create_directory(output_dir, show_logging=True)
    if not os.listdir(output_dir):
        pages_counter = 0
        extracted_pages_filename, extracted_pages_file = DataUtils.open_extracted_bz2_dump_file(pages_counter,
                                                                                                output_dir, lang)

        for page in DataUtils.get_wikipedia_pages(input_filename):
            extracted_pages_file.write(page)
            pages_counter += 1
            if pages_counter % Config.extracted_pages_per_file[lang] == 0:
                LogUtils.logging_pages_extraction(pages_counter, extracted_pages_filename)
                DataUtils.close_extracted_bz2_dump_file(extracted_pages_filename, extracted_pages_file)
                extracted_pages_filename, extracted_pages_file = \
                    DataUtils.open_extracted_bz2_dump_file(pages_counter, output_dir, lang)

        LogUtils.logging_pages_extraction(pages_counter, extracted_pages_filename)
        DataUtils.close_extracted_bz2_dump_file(extracted_pages_filename, extracted_pages_file)
        logging.info('Page Extraction Finished! Number of All Extracted Pages: %d' % pages_counter)


def extract_bz2_dump_information(directory, filename,
                                 extract_abstracts=False,
                                 extract_page_ids=False,
                                 extract_revision_ids=False,
                                 extract_wiki_texts=False,
                                 extract_texts=False,
                                 extract_pages=False,
                                 extract_infoboxes=False,
                                 extract_disambiguations=False,
                                 extract_template_names=False,
                                 lang=None):
    abstracts = dict()
    page_ids = dict()
    revision_ids = dict()
    wiki_texts = dict()
    texts = dict()
    infoboxes = dict()
    pages_with_infobox = dict()
    pages_without_infobox = list()
    disambiguations = list()
    template_names_list = list()

    extract_pages = extract_pages or extract_abstracts or extract_texts or extract_infoboxes or extract_disambiguations

    pages_counter = 0
    input_filename = join(directory, filename)
    for page in DataUtils.get_wikipedia_pages(filename=input_filename):
        parsed_page = DataUtils.parse_page(page)
        pages_counter += 1

        if pages_counter % Config.logging_interval[lang] == 0:
            LogUtils.logging_information_extraction(pages_counter, input_filename)
            gc.collect()

        if extract_template_names and parsed_page.ns.text == '10':
            template_dict = dict()
            template_name, template_type, language = DataUtils.get_template_name_type(parsed_page.title.text)

            template_dict['template_name'] = template_name
            template_dict['type'] = template_type
            template_dict['language_name'] = language
            template_names_list.append(template_dict)

        if parsed_page.ns.text != '0':
            continue

        page_name = parsed_page.title.text
        page_id = parsed_page.id.text
        revision_id = parsed_page.revision.id.text
        extracted_wiki_text = parsed_page.revision.find('text').text
        ref_tag = parsed_page.revision.find('text').find('ref')
        if ref_tag:
            ref_tag.extract()
            extracted_wiki_text_without_ref_tag = parsed_page.revision.find('text').text
        else:
            extracted_wiki_text_without_ref_tag = extracted_wiki_text

        if extract_page_ids:
            page_ids[page_id] = page_name
        if extract_revision_ids:
            revision_ids[page_name] = revision_id
        if extract_wiki_texts:
            wiki_texts[page_name] = extracted_wiki_text

        if extract_pages:
            wiki_text = wtp.parse(extracted_wiki_text)
            wiki_text_without_ref_tag = wtp.parse(extracted_wiki_text_without_ref_tag)
            template_names = wiki_text.templates
            template_names_without_ref_tag = wiki_text_without_ref_tag.templates

            if extract_abstracts:
                first_section = wiki_text.sections[0]
                abstract = first_section.string

                if not any(name in abstract for name in Config.redirect_flags)\
                        and not any(name in page_name for name in Config.disambigution_flags):
                    first_section_templates = first_section.templates
                    for template in first_section_templates:
                        abstract = DataUtils.post_clean(abstract.replace(template.string, ''))

                    abstract = clean(abstract, specify_wikilinks=False)
                    abstracts[page_name] = abstract

            if extract_texts:
                if not any(name in wiki_text.string for name in Config.redirect_flags):
                    texts[page_name] = \
                        DataUtils.post_clean(clean(DataUtils.pre_clean(
                            extracted_wiki_text), specify_wikilinks=False), remove_newline=True)

            page_has_infobox = False
            for template in template_names_without_ref_tag:
                template_name, infobox_type = DataUtils.get_infobox_name_type(template.name)
                if infobox_type:
                    page_has_infobox = True

                    if page_name not in pages_with_infobox:
                        pages_with_infobox[page_name] = list()

                    if template_name not in pages_with_infobox[page_name]:
                        pages_with_infobox[page_name].append(template_name)

                    if extract_infoboxes:
                        if template_name not in infoboxes:
                            infoboxes[template_name] = dict()

                        if page_name not in infoboxes[template_name]:
                            infoboxes[template_name][page_name] = list()

                        infobox = dict()
                        for param in template.arguments:
                            param_name = clean(str(param.name))
                            param_value = DataUtils.post_clean(clean(
                                DataUtils.pre_clean(str(param.value))))
                            if param_value:
                                infobox[param_name] = DataUtils.pre_clean(str(param.value))

                        infoboxes[template_name][page_name].append(infobox)

            if not page_has_infobox:
                pages_without_infobox.append(page_name)

            if extract_disambiguations:
                disambiguation_dict = dict()
                for flag in Config.disambigution_flags:
                    if any(flag in DataUtils.get_template_name_type(template.name)[0] for template in template_names):
                        disambiguation_dict['title'] = page_name
                        disambiguation_dict['field'] = \
                            DataUtils.get_disambiguation_links_regular(str(extracted_wiki_text))
                        disambiguations.append(disambiguation_dict)
                        break

            del template_names
            del wiki_text

    if extract_abstracts:
        DataUtils.save_json(Config.extracted_abstracts_dir, filename, abstracts)
        if extract_pages:
            DataUtils.save_json(Config.extracted_with_infobox_dir, DataUtils.get_abstracts_filename(filename),
                                abstracts, filter_dict=pages_with_infobox)
            DataUtils.save_json(Config.extracted_without_infobox_dir, DataUtils.get_abstracts_filename(filename),
                                abstracts, filter_dict=pages_without_infobox)

    if extract_page_ids:
        DataUtils.save_json(Config.extracted_page_ids_dir, filename, page_ids)

    if extract_revision_ids:
        DataUtils.save_json(Config.extracted_revision_ids_dir, filename, revision_ids)
        if extract_pages:
            DataUtils.save_json(Config.extracted_with_infobox_dir, DataUtils.get_revision_ids_filename(filename),
                                revision_ids, filter_dict=pages_with_infobox)
            DataUtils.save_json(Config.extracted_without_infobox_dir, DataUtils.get_revision_ids_filename(filename),
                                revision_ids, filter_dict=pages_without_infobox)

    if extract_wiki_texts:
        DataUtils.save_json(Config.extracted_wiki_texts_dir, filename, wiki_texts)
        if extract_pages:
            DataUtils.save_json(Config.extracted_with_infobox_dir, DataUtils.get_wiki_texts_filename(filename),
                                wiki_texts, filter_dict=pages_with_infobox)
            DataUtils.save_json(Config.extracted_without_infobox_dir, DataUtils.get_wiki_texts_filename(filename),
                                wiki_texts, filter_dict=pages_without_infobox)

    if extract_texts:
        DataUtils.save_json(Config.extracted_texts_dir, filename, texts)
        if extract_pages:
            DataUtils.save_json(Config.extracted_with_infobox_dir, DataUtils.get_texts_filename(filename),
                                texts, filter_dict=pages_with_infobox)
            DataUtils.save_json(Config.extracted_without_infobox_dir, DataUtils.get_texts_filename(filename),
                                texts, filter_dict=pages_without_infobox)

    if extract_pages:
        DataUtils.save_json(Config.extracted_pages_with_infobox_dir[lang], filename, pages_with_infobox)
        DataUtils.save_json(Config.extracted_pages_without_infobox_dir[lang], filename, pages_without_infobox)

    if extract_infoboxes:
        DataUtils.save_json(Config.extracted_with_infobox_dir, DataUtils.get_infoboxes_filename(filename), infoboxes)

    if extract_disambiguations:
        DataUtils.save_json(Config.extracted_disambiguations_dir, filename, disambiguations)

    if extract_template_names:
        DataUtils.save_json(Config.extracted_template_names_dir[lang], filename, template_names_list)

    LogUtils.logging_information_extraction(pages_counter, input_filename)


def extract_fawiki_latest_pages_articles_dump():
    extract_bz2_dump('fa')


def extract_enwiki_latest_pages_articles_dump():
    extract_bz2_dump('en')


def multiprocess_extraction(lang, parameters):
    parameters['lang'] = lang
    directory = Config.extracted_pages_articles_dir[lang]
    filenames = os.listdir(directory)
    if filenames:
        Parallel(n_jobs=-1)(delayed(extract_bz2_dump_information)(directory, filename, **parameters)
                            for filename in filenames)


def extract_fawiki_abstracts():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_abstracts'] = True
    multiprocess_extraction('fa', parameters)


def extract_fawiki_page_ids():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_page_ids'] = True
    multiprocess_extraction('fa', parameters)


def extract_fawiki_revision_ids():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_revision_ids'] = True
    multiprocess_extraction('fa', parameters)


def extract_fawiki_wiki_texts():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_wiki_texts'] = True
    multiprocess_extraction('fa', parameters)


def extract_fawiki_texts():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_texts'] = True
    multiprocess_extraction('fa', parameters)


def extract_fawiki_pages():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_pages'] = True
    multiprocess_extraction('fa', parameters)


def extract_fawiki_infoboxes():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_infoboxes'] = True
    multiprocess_extraction('fa', parameters)


def extract_fawiki_disambiguations():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_disambiguations'] = True
    multiprocess_extraction('fa', parameters)


def extract_fawiki_template_names():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_template_names'] = True
    multiprocess_extraction('fa', parameters)


def extract_enwiki_pages():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_pages'] = True
    multiprocess_extraction('en', parameters)


def extract_enwiki_template_names():
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_template_names'] = True
    multiprocess_extraction('en', parameters)


def extract_fawiki_bz2_dump_information():
    extract_fawiki_latest_pages_articles_dump()
    parameters = copy.deepcopy(Config.extract_bz2_dump_information_parameters)
    parameters['extract_abstracts'] = True
    parameters['extract_page_ids'] = True
    parameters['extract_revision_ids'] = True
    parameters['extract_wiki_texts'] = True
    parameters['extract_texts'] = True
    parameters['extract_pages'] = True
    parameters['extract_infoboxes'] = True
    parameters['extract_disambiguations'] = True
    parameters['extract_template_names'] = True
    multiprocess_extraction('fa', parameters)


def extract_page_ids_from_sql_dump():
    all_records = SqlUtils.get_sql_rows(Config.fawiki_latest_page_dump)

    page_ids = dict()
    for record in all_records:
        for columns in record:
            page_id, page_namespace, page_title = columns[0], columns[1], columns[2]
            page_ids[page_id] = page_title

    DataUtils.save_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename, page_ids)


def extract_lang_links_from_sql_dump():
    lang_links_en = dict()
    lang_links_ar = dict()
    page_ids = DataUtils.load_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename)

    all_records = SqlUtils.get_sql_rows(Config.fawiki_latest_lang_links_dump)
    for record in all_records:
        for columns in record:
            ll_from, ll_lang, ll_title = columns[0], columns[1], columns[2]
            if ll_from in page_ids:
                ll_from = page_ids[ll_from]

                if ll_lang == 'en':
                    lang_links_en[ll_from] = ll_title
                elif ll_lang == 'ar':
                    lang_links_ar[ll_from] = ll_title

    DataUtils.save_json(Config.extracted_lang_links_dir, Config.extracted_en_lang_link_filename, lang_links_en)
    DataUtils.save_json(Config.extracted_lang_links_dir, Config.extracted_ar_lang_link_filename, lang_links_ar)


def extract_redirects_from_sql_dump():
    redirects = dict()
    reverse_redirects = dict()
    page_ids = DataUtils.load_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename)

    all_records = SqlUtils.get_sql_rows(Config.fawiki_latest_redirect_dump)
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
        DataUtils.save_json(Config.extracted_redirects_dir, DataUtils.get_redirects_filename(ns), redirects[ns])
    for ns in reverse_redirects:
        DataUtils.save_json(Config.extracted_reverse_redirects_dir, DataUtils.get_redirects_filename(ns),
                            reverse_redirects[ns])

    sql_dump = SqlUtils.get_wiki_template_redirect_sql_dump(redirects['10'])
    SqlUtils.save_sql_dump(Config.extracted_redirects_dir, '10-redirects.sql', sql_dump)


def extract_image_names_from_sql_dump():
    image_names_types = dict()

    all_records = SqlUtils.get_sql_rows(Config.fawiki_latest_images_dump, quotechar='"')
    for record in all_records:
        for columns in record:
            image_name, image_type = columns[0].strip("'"), columns[8].strip("'")
            image_names_types[image_name] = image_type

    DataUtils.save_json(Config.extracted_image_names_types_dir,
                        Config.extracted_image_names_types_filename, image_names_types)


def extract_category_links_from_sql_dump(page_ids, output_directory, output_filename):
    category_links = defaultdict(list)

    all_records = SqlUtils.get_sql_rows(Config.fawiki_latest_category_links_dump)
    for record in all_records:
        for columns in record:
            cl_from, cl_to = columns[0], columns[1]
            if cl_from in page_ids:
                cl_from = page_ids[cl_from]
                category_links[cl_from].append(cl_to)

    DataUtils.save_json(output_directory, output_filename, category_links)


def extract_external_links_from_sql_dump(page_ids, output_directory, output_filename):
    external_links = defaultdict(list)

    all_records = SqlUtils.get_sql_rows(Config.fawiki_latest_external_links_dump)
    for record in all_records:
        for columns in record:
            el_from, el_to = columns[1], columns[3]
            if el_from in page_ids:
                el_from = page_ids[el_from]
                external_links[el_from].append(el_to)

    DataUtils.save_json(output_directory, output_filename, external_links)


def extract_wiki_links_from_sql_dump(page_ids, output_directory, output_filename):
    wiki_links = defaultdict(list)

    all_records = SqlUtils.get_sql_rows(Config.fawiki_latest_page_links_dump)
    for record in all_records:
        for columns in record:
            pl_from, pl_title = columns[0], columns[2]
            if pl_from in page_ids:
                pl_from = page_ids[pl_from]
                wiki_links[pl_from].append(pl_title)

    DataUtils.save_json(output_directory, output_filename, wiki_links)


def extract_category_external_wiki_links_from_sql_dumps():
    page_ids = DataUtils.load_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename)

    extract_category_links_from_sql_dump(page_ids, Config.extracted_category_links_dir,
                                         Config.extracted_category_links_filename)

    extract_external_links_from_sql_dump(page_ids,
                                         Config.extracted_external_links_dir, Config.extracted_external_links_filename)

    del page_ids

    page_ids_filenames = os.listdir(Config.extracted_page_ids_dir)
    page_ids_filenames.remove(Config.extracted_page_ids_filename+'.json')

    for page_ids_filename in page_ids_filenames:
        page_ids = DataUtils.load_json(Config.extracted_page_ids_dir, page_ids_filename)

        extract_wiki_links_from_sql_dump(page_ids, Config.extracted_wiki_links_dir, page_ids_filename)


def extract_category_links():
    page_ids = DataUtils.load_json(Config.extracted_page_ids_dir, Config.extracted_page_ids_filename)

    extract_category_links_from_sql_dump(page_ids, Config.extracted_category_links_dir,
                                         Config.extracted_category_links_filename)


def extraction_for_update():
    extract_fawiki_bz2_dump_information()
    extract_image_names_from_sql_dump()
    extract_page_ids_from_sql_dump()
    extract_redirects_from_sql_dump()
    extract_category_links()