import os
from collections import OrderedDict, defaultdict
from os.path import join

import Config
import DataUtils
import SqlUtils


def reorganize_infoboxes():
    reorganized_infoboxes = dict()
    directory = Config.extracted_with_infobox_dir
    filenames = [filename for filename in os.listdir(directory) if DataUtils.is_infobox_file(filename)]
    for filename in filenames:
        infoboxes = DataUtils.load_json(directory, filename)
        for infobox_name in infoboxes:
            template_name, infobox_type = DataUtils.get_infobox_name_type(infobox_name)
            if infobox_type not in reorganized_infoboxes:
                reorganized_infoboxes[infobox_type] = dict()
            if infobox_name not in reorganized_infoboxes[infobox_type]:
                reorganized_infoboxes[infobox_type][infobox_name] = dict()

            for page_name in infoboxes[infobox_name]:
                reorganized_infoboxes[infobox_type][infobox_name][page_name] = infoboxes[infobox_name][page_name]

    for infobox_type in reorganized_infoboxes:
        for infobox_name in reorganized_infoboxes[infobox_type]:
            infobox_name_type_path = join(Config.reorganized_infoboxes_dir, infobox_type, infobox_name)
            DataUtils.save_json(infobox_name_type_path, 'infoboxes',
                                reorganized_infoboxes[infobox_type][infobox_name])


def build_infobox_tuples():
    directory = Config.extracted_with_infobox_dir
    infoboxes_filenames = sorted([filename for filename in os.listdir(directory)
                                  if DataUtils.is_infobox_file(filename)])
    revision_ids_filenames = sorted([filename for filename in os.listdir(directory)
                                     if DataUtils.is_revision_ids_file(filename)])
    for infobox_filename, revision_ids_filename in zip(infoboxes_filenames, revision_ids_filenames):
        tuples = list()
        infoboxes = DataUtils.load_json(directory, infobox_filename)
        revision_ids = DataUtils.load_json(directory, revision_ids_filename)
        for infobox_name in infoboxes:
            template_name, infobox_type = DataUtils.get_infobox_name_type(infobox_name)
            for page_name in infoboxes[infobox_name]:
                for infobox in infoboxes[infobox_name][page_name]:
                    for predicate, values in infobox.items():
                        for value in values:
                            json_dict = dict()
                            json_dict['template_name'] = infobox_name
                            json_dict['template_type'] = infobox_type
                            json_dict['subject'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
                            json_dict['predicate'] = predicate
                            json_dict['object'] = value
                            json_dict['source'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
                            json_dict['version'] = revision_ids[page_name]
                            tuples.append(json_dict)
        DataUtils.save_json(Config.final_tuples_dir, infobox_filename, tuples)


def count_infobox_tuples():
    counter = 0
    direcory = Config.final_tuples_dir
    tuples_filenames = os.listdir(direcory)
    for filename in tuples_filenames:
        counter += len(DataUtils.load_json(direcory, filename))

    print(counter)


def count_number_of_each_infobox():
    infobox_counters = dict()
    templates_transcluded = list()
    pages_with_infobox = os.listdir(Config.extracted_pages_with_infobox_dir)
    for filename in pages_with_infobox:
        pages = DataUtils.load_json(Config.extracted_pages_with_infobox_dir, filename)
        for page in pages:
            for infobox_name in pages[page]:
                if infobox_name in infobox_counters:
                    infobox_counters[infobox_name] += 1
                else:
                    infobox_counters[infobox_name] = 1

    for infobox_name in infobox_counters:
        template_name, infobox_type = DataUtils.get_infobox_name_type(infobox_name)
        templates_transcluded.append({'template_name': template_name,
                                      'template_type': infobox_type,
                                      'language': DataUtils.get_infobox_lang(infobox_type),
                                      'count': infobox_counters[infobox_name]})

    DataUtils.save_json(Config.infobox_counters_dir, 'infobox_counters',
                        sorted(templates_transcluded, key=lambda item: item['count'], reverse=True), sort_keys=False)

    order = Config.wiki_templates_transcluded_on_pages_key_order
    sql_dump = SqlUtils.get_wiki_templates_transcluded_on_pages_sql_dump(templates_transcluded, order)
    SqlUtils.save_sql_dump(Config.infobox_counters_dir, 'infobox_counters.sql', sql_dump)


def aggregate_infobox_properties():
    properties = dict()
    directory = Config.extracted_with_infobox_dir
    infoboxes_filenames = [filename for filename in os.listdir(directory) if DataUtils.is_infobox_file(filename)]
    for infoboxes_filename in infoboxes_filenames:
        infoboxes = DataUtils.load_json(directory, infoboxes_filename)
        for infobox_name in infoboxes:
            for page_name in infoboxes[infobox_name]:
                for infobox in infoboxes[infobox_name][page_name]:
                    for predicate in infobox:
                        if predicate in properties:
                            properties[predicate] += 1
                        else:
                            properties[predicate] = 1

    DataUtils.save_json(Config.infobox_predicates_dir, 'infobox_predicates',
                        OrderedDict(sorted(properties.items(), key=lambda item: item[1], reverse=True)),
                        sort_keys=False)


def template_redirect_with_fa():
    redirects = DataUtils.load_json(Config.extracted_redirects_dir, DataUtils.get_redirects_filename('10'))
    with_fa_redirects = defaultdict(list)

    for redirect_from, redirect_to in redirects.items():
        if DataUtils.is_ascii(redirect_from):
            if DataUtils.without_en_chars(redirect_to):
                with_fa_redirects[redirect_to].append(redirect_from)
        elif DataUtils.without_en_chars(redirect_from):
            if DataUtils.is_ascii(redirect_to):
                with_fa_redirects[redirect_from].append(redirect_to)

    DataUtils.save_json(Config.extracted_redirects_dir, '10-redirects-with-fa', with_fa_redirects)


def mapping_sql():
    table_name = Config.wiki_template_mapping_table_name
    table_structure = Config.wiki_template_mapping_table_structure

    key_order = Config.wiki_template_mapping_key_order
    primary_keys = Config.wiki_template_mapping_primary_keys
    unique_keys = Config.wiki_template_mapping_unique_keys

    ordered_table_structure = SqlUtils.create_order_structure(table_structure, key_order)

    command = SqlUtils.sql_create_table_command_generator(table_name, ordered_table_structure,
                                                          primary_key=primary_keys,
                                                          unique_key=unique_keys)
    SqlUtils.execute_command_mysql(command)

    redirect_data = DataUtils.load_json(Config.extracted_redirects_dir, '10-redirects-with-fa')
    mapping_data = DataUtils.load_json(Config.extracted_infobox_mapping_dir, Config.extracted_infobox_mapping_filename)

    for redirect_from, redirects in redirect_data.items():
        for redirect_to in redirects:
            row = [{Config.wiki_template_mapping_key_order[1]: redirect_from,
                    Config.wiki_template_mapping_key_order[2]: redirect_to,
                    Config.wiki_template_mapping_key_order[4]: 'Redirect'}]
            query = SqlUtils.insert_command(ordered_table_structure, table_name, key_order[1:3] + key_order[4:],
                                            row)
            SqlUtils.execute_command_mysql(query)

    for fa_infobox, en_infoboxes in mapping_data.items():
        for en_infobox in en_infoboxes:
            row = [{Config.wiki_template_mapping_key_order[1]: fa_infobox,
                    Config.wiki_template_mapping_key_order[2]: en_infobox,
                    Config.wiki_template_mapping_key_order[4]: 'Interlingual'}]
            query = SqlUtils.insert_command(table_structure, table_name, key_order[1:3] + key_order[4:], row)
            SqlUtils.execute_command_mysql(query)