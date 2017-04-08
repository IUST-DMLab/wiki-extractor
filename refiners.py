import json
import operator
import os
import re
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
    pages_with_infobox = os.listdir(Config.extracted_pages_with_infobox_dir['fa'])
    for filename in pages_with_infobox:
        pages = DataUtils.load_json(Config.extracted_pages_with_infobox_dir['fa'], filename)
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


def get_fa_en_infobox_mapping():
    fa_en_infobox_mapping = defaultdict(list)

    en_lang_links = DataUtils.load_json(Config.extracted_lang_links_dir, Config.extracted_en_lang_link_filename)
    fawiki_fa_infoboxes = DataUtils.get_fawiki_fa_infoboxes()
    enwiki_infoboxes = DataUtils.get_enwiki_infoboxes()

    for fa_page_name, fa_infoboxes in fawiki_fa_infoboxes.items():
        fa_page_name = fa_page_name.replace(' ', '_')
        if fa_page_name in en_lang_links:
            en_page_name = en_lang_links[fa_page_name]
            if en_page_name in enwiki_infoboxes:
                en_infoboxes = enwiki_infoboxes[en_page_name]
                for fa_infobox in fa_infoboxes:
                    for en_infobox in en_infoboxes:
                        if en_infobox not in fa_en_infobox_mapping[fa_infobox]:
                            fa_en_infobox_mapping[fa_infobox].append(en_infobox)

    DataUtils.save_json(Config.infobox_mapping_dir, Config.infobox_mapping_filename, fa_en_infobox_mapping)


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
    mapping_data = DataUtils.load_json(Config.infobox_mapping_dir, Config.infobox_mapping_filename)

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


def get_file_list_from_dir(dir_name):

    file_list = []
    for root, dirs, file_name in os.walk(dir_name):
        for filename in file_name:
            real_path = os.path.join(root, filename)
            file_list.append(real_path)
    return file_list


def create_table_mysql_template(table_name, dir_path):

    main_list = get_file_list_from_dir(dir_path)

    table_structure = OrderedDict([('id', 'int(10) NOT NULL AUTO_INCREMENT'), ('template_name', 'varchar(250)'),
                                   ('type', 'varchar(250)'), ('language_name', 'varchar(250)')])

    insert_columns = ['template_name', 'type', 'language_name']

    primary_key = ['id']

    command = SqlUtils.sql_create_table_command_generator(table_name, table_structure, primary_key,
                                                          drop_table=True)

    message = 'some exception occur while insert table wiki_farsi_templates'
    SqlUtils.execute_command_mysql(command, message)

    command = ""
    count = 0
    for my_id, dstFile in enumerate(main_list):

        count += 1
        message = 'some exception occur in file ' + str(count)
        print('file number '+dstFile+' write \n')

        mapping_infobox = open(dstFile)
        my_row = json.load(mapping_infobox)

        command += SqlUtils.insert_command(table_structure, table_name, insert_columns, my_row)
        SqlUtils.execute_command_mysql(command, message)

        command = ""


def check_path(my_dict, path_names, page_n):

    regex = re.compile(
        r'(?:^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$)', re.IGNORECASE)

    pic_prefix = ['jpg', 'tif', 'tiff', 'gif', 'png', 'jpeg', 'svg', 'exif',
                  'bmp', 'ppm', 'pgm', 'pbm', 'pnm', 'webp', 'heif', 'bat']

    for myKey in my_dict:

        my_value = my_dict[myKey]
        if isinstance(my_value, dict):
            check_path(my_value, path_names, page_n)
        else:
            result = regex.match(my_value)
            if not(str(result) == 'None'):
                if not (any(s in my_value.lower() for s in pic_prefix)):
                    if myKey in path_names.keys():
                        count = path_names[myKey][0]
                        my_word = [(count + 1), my_value]
                        path_names[myKey] = my_word

                    else:
                        my_word = [1, my_value]
                        path_names[myKey] = my_word


def check_image(my_dict, image_names, page_n):

    pic_prefix = ['jpg', 'tif', 'tiff', 'gif', 'png', 'jpeg', 'svg', 'exif',
                  'bmp', 'ppm', 'pgm', 'pbm', 'pnm', 'webp', 'heif', 'bat']

    for my_Key in my_dict:
        my_value = my_dict[my_Key]
        if isinstance(my_value, dict):
            check_image(my_value, image_names, page_n)
        else:

            for s in pic_prefix:
                if s in my_value.lower():

                    if my_Key in image_names.keys():
                        count = image_names[my_Key]
                        image_names[my_Key] = count + 1
                    else:
                        image_names[my_Key] = 1
                    break


def get_attribute_name(attribute_type):

    dir_path = Config.extracted_dir + '/infoboxes'
    main_list = get_file_list_from_dir(dir_path)

    attribute_names = {}

    for my_id, dstFile in enumerate(main_list):

        infobox = open(dstFile)
        data = json.load(infobox)

        for page_title, pageInfo in data.items():
            if attribute_type == 'image':
                check_image(pageInfo, attribute_names, page_title)
            else:
                check_path(pageInfo, attribute_names, page_title)

    return attribute_names


def get_image_name(filename):
    att_name = get_attribute_name('image')
    sorted_image_names = sorted(att_name.items(), key=operator.itemgetter(1), reverse=True)
    DataUtils.save_json(Config.extracted_image_name_dir, filename, sorted_image_names)


def get_path_name(filename):
    att_name = get_attribute_name('path')
    sorted_path_names = sorted(att_name.items(), key=lambda i: i[1][0], reverse=True)
    DataUtils.save_json(Config.extracted_path_name_dir, filename, sorted_path_names)


if __name__ == '__main__':
    create_table_mysql_template('wiki_en_templates', Config.extracted_template_names_dir['en'])
    get_image_name('image_name')
    get_path_name('path_name')
