import os
import csv
from collections import OrderedDict, defaultdict
from hashlib import md5
from os.path import join
import re

import Config
import DataUtils
import SqlUtils
import hazm


def reorganize_infoboxes():
    reorganized_infoboxes = dict()
    directory = Config.extracted_with_infobox_dir
    filenames = DataUtils.get_infoboxes_filenames(directory)
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
    infoboxes_filenames = DataUtils.get_infoboxes_filenames(directory)
    revision_ids_filenames = DataUtils.get_revision_ids_filenames(directory)
    image_names_types_in_fawiki = DataUtils.load_json(Config.extracted_image_names_types_dir,
                                                      Config.extracted_image_names_types_filename)
    for infobox_filename, revision_ids_filename in zip(infoboxes_filenames, revision_ids_filenames):
        tuples = list()
        infoboxes = DataUtils.load_json(directory, infobox_filename)
        revision_ids = DataUtils.load_json(directory, revision_ids_filename)
        for infobox_name in infoboxes:
            template_name, infobox_type = DataUtils.get_infobox_name_type(infobox_name)
            for page_name in infoboxes[infobox_name]:
                for infobox in infoboxes[infobox_name][page_name]:
                    for predicate, values in infobox.items():
                        if len(predicate) < 255:
                            for value in DataUtils.split_infobox_values(values):
                                if DataUtils.is_image(value):
                                    value = DataUtils.clean_image_value(value, image_names_types_in_fawiki)
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


def build_abstract_tuples():
    abstracts_directory = Config.extracted_abstracts_dir
    revision_ids_directory = Config.extracted_revision_ids_dir
    abstracts_filenames = sorted(os.listdir(abstracts_directory))
    revision_ids_filenames = sorted(os.listdir(revision_ids_directory))
    for abstract_filename, revision_ids_filename in zip(abstracts_filenames, revision_ids_filenames):
        tuples = list()
        abstracts = DataUtils.load_json(abstracts_directory, abstract_filename)
        revision_ids = DataUtils.load_json(revision_ids_directory, revision_ids_filename)
        for page_name in abstracts:
            json_dict = dict()
            json_dict['template_name'] = None
            json_dict['template_type'] = None
            json_dict['subject'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
            json_dict['predicate'] = 'abstract'
            json_dict['object'] = abstracts[page_name]
            json_dict['source'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
            json_dict['version'] = revision_ids[page_name]
            tuples.append(json_dict)
        DataUtils.save_json(Config.final_abstract_tuples_dir, abstract_filename, tuples)


def build_category_tuples():
    category_directory = Config.extracted_category_links_dir
    category_filename = Config.extracted_category_links_filename
    categories = DataUtils.load_json(category_directory, category_filename)
    revision_ids_directory = Config.extracted_revision_ids_dir
    revision_ids_filenames = sorted(os.listdir(revision_ids_directory))
    for revision_ids_filename in revision_ids_filenames:
        revision_ids = DataUtils.load_json(revision_ids_directory, revision_ids_filename)

        tuples = list()
        for page_name in revision_ids:
            page_name = page_name.replace(' ', '_')
            if page_name in categories:
                page_categories = categories[page_name]
                for page_category in page_categories:
                    json_dict = dict()
                    json_dict['template_name'] = None
                    json_dict['template_type'] = None
                    json_dict['subject'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
                    json_dict['predicate'] = 'wikiCategory'
                    json_dict['object'] = page_category.replace('_', ' ')
                    json_dict['source'] = 'http://fa.wikipedia.org/wiki/' + page_name.replace(' ', '_')
                    json_dict['version'] = revision_ids[page_name.replace('_', ' ')]
                    tuples.append(json_dict)
        DataUtils.save_json(Config.final_category_tuples_dir, revision_ids_filename, tuples)


def count_infobox_tuples():
    counter = 0
    direcory = Config.final_tuples_dir
    tuples_filenames = os.listdir(direcory)
    for filename in tuples_filenames:
        counter += len(DataUtils.load_json(direcory, filename))

    print("infobox tuples: ",  counter)


def count_category_tuples():
    counter = 0
    direcory = Config.final_category_tuples_dir
    tuples_filenames = os.listdir(direcory)
    for filename in tuples_filenames:
        counter += len(DataUtils.load_json(direcory, filename))

    print("infobox tuples: ",  counter)


def count_entities():
    with_infobox_filenames = os.listdir(Config.extracted_with_infobox_dir)
    without_infobox_filenames = os.listdir(Config.extracted_without_infobox_dir)
    with_infobox_without_redirects_counter = without_infobox_without_redirects_counter = 0
    with_infobox_with_redirects_counter = without_infobox_with_redirects_counter = 0

    for filename in with_infobox_filenames:
        if 'abstracts' in filename:
            with_infobox_without_redirects_counter += len(
                DataUtils.load_json(Config.extracted_with_infobox_dir, filename))
        if 'revision_ids' in filename:
            with_infobox_with_redirects_counter += len(DataUtils.load_json(Config.extracted_with_infobox_dir, filename))

    for filename in without_infobox_filenames:
        if 'abstracts' in filename:
            without_infobox_without_redirects_counter += len(
                DataUtils.load_json(Config.extracted_without_infobox_dir, filename))
        if 'revision_ids' in filename:
            without_infobox_with_redirects_counter += len(
                DataUtils.load_json(Config.extracted_without_infobox_dir, filename))

    print("with infobox entities (redirects ignored): %d \nwithout_infobox entities (redirects ignored): %d" % (
        with_infobox_without_redirects_counter, without_infobox_without_redirects_counter))

    print("with infobox entities (with redirects): %d \nwithout_infobox entities (with_redirects):%d" % (
        with_infobox_with_redirects_counter, without_infobox_with_redirects_counter))


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
    infoboxes_filenames = DataUtils.get_infoboxes_filenames(directory)
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


def find_sequence_property():
    infobox_properties_map = defaultdict(list)
    directory = Config.extracted_with_infobox_dir
    infoboxes_filenames = DataUtils.get_infoboxes_filenames(directory)
    for infoboxes_filename in infoboxes_filenames:
        infoboxes = DataUtils.load_json(directory, infoboxes_filename)
        for infobox_name in infoboxes:
            for page_name in infoboxes[infobox_name]:
                for infobox in infoboxes[infobox_name][page_name]:
                    for predicate in infobox:
                        if DataUtils.contains_digits(predicate)\
                                and predicate not in infobox_properties_map[infobox_name]:
                            infobox_properties_map[infobox_name].append(predicate)
    for infobox_name in infobox_properties_map:
        infobox_properties_map[infobox_name] = sorted(infobox_properties_map[infobox_name])

    DataUtils.save_json(Config.infobox_predicates_dir, 'infobox_predicates_with_digits', infobox_properties_map)


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


def get_fa_pages_with_infobox_without_en_page():
    fa_pages_with_infobox_without_en_page = list()

    en_lang_links = DataUtils.load_json(Config.extracted_lang_links_dir, Config.extracted_en_lang_link_filename)
    fawiki_fa_infoboxes = DataUtils.get_fawiki_fa_infoboxes()

    for fa_page_name, fa_infoboxes in fawiki_fa_infoboxes.items():
        fa_page_name = fa_page_name.replace(' ', '_')
        if fa_page_name not in en_lang_links:
            fa_pages_with_infobox_without_en_page.append(fa_page_name)

    DataUtils.save_json(Config.fa_pages_with_infobox_without_en_page_dir,
                        Config.fa_pages_with_infobox_without_en_page_filename, fa_pages_with_infobox_without_en_page)


def get_fa_pages_with_infobox_without_en_infobox():
    fa_pages_with_infobox_without_en_infobox = list()

    en_lang_links = DataUtils.load_json(Config.extracted_lang_links_dir, Config.extracted_en_lang_link_filename)
    fawiki_fa_infoboxes = DataUtils.get_fawiki_fa_infoboxes()
    enwiki_infoboxes = DataUtils.get_enwiki_infoboxes()

    for fa_page_name, fa_infoboxes in fawiki_fa_infoboxes.items():
        fa_page_name = fa_page_name.replace(' ', '_')
        if fa_page_name in en_lang_links:
            en_page_name = en_lang_links[fa_page_name]
            if en_page_name not in enwiki_infoboxes:
                fa_pages_with_infobox_without_en_infobox.append(fa_page_name)

    DataUtils.save_json(Config.fa_pages_with_infobox_without_en_infobox_dir,
                        Config.fa_pages_with_infobox_without_en_infobox_filename,
                        fa_pages_with_infobox_without_en_infobox)


def find_properties_with_url_images():
    properties_with_url = dict()
    properties_with_images = dict()
    directory = Config.extracted_with_infobox_dir
    filenames = DataUtils.get_infoboxes_filenames(directory)
    for filename in filenames:
        infoboxes = DataUtils.load_json(directory, filename)
        for infobox_name in infoboxes:
            for page_name in infoboxes[infobox_name]:
                for infobox in infoboxes[infobox_name][page_name]:
                    for predicate, values in infobox.items():
                        value = values[0]
                        if DataUtils.is_url(value):
                            if predicate in properties_with_url:
                                properties_with_url[predicate] += 1
                            else:
                                properties_with_url[predicate] = 1
                        if DataUtils.is_image(value):
                            if predicate in properties_with_images:
                                properties_with_images[predicate] += 1
                            else:
                                properties_with_images[predicate] = 1

    DataUtils.save_json(Config.infobox_properties_with_url_dir, Config.infobox_properties_with_url_filename,
                        OrderedDict(sorted(properties_with_url.items(), key=lambda item: item[1], reverse=True)),
                        sort_keys=False)
    DataUtils.save_json(Config.infobox_properties_with_images_dir, Config.infobox_properties_with_images_filename,
                        OrderedDict(sorted(properties_with_images.items(), key=lambda item: item[1], reverse=True)),
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


def create_table_mysql_template():
    directory = Config.extracted_template_names_dir['en']
    template_names_filenames = os.listdir(directory)
    table_name = Config.wiki_en_templates_table_name

    table_structure = SqlUtils.create_order_structure(Config.wiki_en_templates_table_structure,
                                                      Config.wiki_en_templates_key_order)
    primary_key = Config.wiki_en_templates_primary_key
    insert_columns = ['template_name', 'type', 'language_name']

    command = SqlUtils.sql_create_table_command_generator(table_name, table_structure,
                                                          primary_key=primary_key, drop_table=True)

    SqlUtils.execute_command_mysql(command)

    for filename in template_names_filenames:
        my_row = DataUtils.load_json(directory, filename)

        command = SqlUtils.insert_command(table_structure, table_name, insert_columns, my_row)
        SqlUtils.execute_command_mysql(command)


def get_articles_names(farsnet_words=Config.farsnet_words_filename):
    directory = Config.extracted_texts_dir
    text_filenames = os.listdir(directory)
    article_words_count = dict()
    farsnet_words = DataUtils.line_to_list(Config.resources_dir, farsnet_words)

    for filename in text_filenames:
        data = DataUtils.load_json(directory, filename)
        for page_name, text in data.items():
            article_words_count[page_name] = len(text.split())

    DataUtils.create_directory(Config.article_names_dir)

    with open(join(Config.article_names_dir, Config.article_names_filename), 'w+',
              encoding='utf8') as article_names_file:
        for article in OrderedDict(sorted(article_words_count.items(), key=lambda item: item[1], reverse=True)):
            article_names_file.write(article + '\n')

    article_words_count_in_farsnet = dict()
    for word in farsnet_words:
        if word in article_words_count:
            article_words_count_in_farsnet[word] = article_words_count[word]

    with open(join(Config.article_names_dir, Config.article_names_in_farsnet_filename), 'w+',
              encoding='utf8') as article_names_in_farsnet_file:
        for article in OrderedDict(sorted(article_words_count_in_farsnet.items(), key=lambda item: item[1],
                                          reverse=True)):
            article_names_in_farsnet_file.write(article + '\n')


def remove_duplicate_from_farsnet():
    input_filename = join(Config.resources_dir, Config.farsnet_csv)
    output_filename = join(Config.article_names_dir, Config.farsnet_csv_unique_id)
    farsnet_unique_words = join(Config.article_names_dir, Config.farsnet_unique_ids_words_filename)

    ids = []
    farsnet_word = list()
    with open(input_filename, 'r') as input_file, open(output_filename, 'w') as output_file,\
            open(farsnet_unique_words, 'w') as farsnet_words_file:
        csv_reader, csv_writer = csv.reader(input_file), csv.writer(output_file)
        csv_writer.writerow(next(csv_reader))
        for line in csv_reader:
            if line[3] not in ids:
                csv_writer.writerow(line)
                if line[1] not in farsnet_word:
                    farsnet_word.append(line[1])
                ids.append(line[3])

        for word in farsnet_word:
            farsnet_words_file.write(word)
            farsnet_words_file.write('\n')


def get_farsnet_names_ids():
    input_filename = join(Config.article_names_dir, Config.farsnet_csv_unique_id)
    output_filename = join(Config.article_names_dir, Config.article_names_ids_in_farsnet_csv_filename)

    get_articles_names(farsnet_words=Config.farsnet_unique_ids_words_filename)
    article_names_in_farsnet = DataUtils.line_to_list(Config.article_names_dir,
                                                      Config.article_names_in_farsnet_filename)
    names_ids = dict()
    with open(input_filename, 'r') as input_file, open(output_filename, 'w') as output_file:
        csv_reader, csv_writer = csv.reader(input_file), csv.writer(output_file)
        csv_writer.writerow(next(csv_reader))
        for line in csv_reader:
            if line[1].strip() in article_names_in_farsnet:
                csv_writer.writerow(line)
                names_ids[line[1]] = line[3]

    DataUtils.save_json(Config.article_names_dir, Config.article_names_ids_in_farsnet_json_filename, names_ids)


def get_ambiguation_farsnet_word():
    input_farsnet_unique_id = join(Config.article_names_dir, Config.farsnet_csv_unique_id)
    output_filename = join(Config.article_names_dir, Config.farsnet_ambiguate_word_filename)

    map_farsnet_list = DataUtils.load_json(Config.article_names_dir, Config.article_names_ids_in_farsnet_json_filename)
    map_farsnet_list_keys = list(map_farsnet_list.keys())

    write_line = False
    with open(input_farsnet_unique_id, 'r') as farsnet_unique_id, open(output_filename, 'w') as output_file:
        csv_reader, csv_writer = csv.reader(farsnet_unique_id), csv.writer(output_file)

        temp_list = ['word', 'defaultValue', 'id', 'senses_snapshot', 'gloss', 'example']
        csv_writer.writerow(temp_list)

        temp_list = []
        for line in csv_reader:
            if temp_list and line[0] == temp_list[0]:
                csv_writer.writerow(temp_list)
                write_line = True
            elif write_line:
                csv_writer.writerow(temp_list)
                write_line = False

            if line[1].strip() in map_farsnet_list_keys:
                del temp_list[:]
                temp_list.append(line[0])
                temp_list.append(line[1])
                temp_list.append(line[3])
                temp_list.append(line[4])
                temp_list.append(line[5])
                temp_list.append(line[6])


def similar(s1, s2):
    normalizer = hazm.Normalizer()
    s1 = normalizer.normalize(s1)
    s2 = normalizer.normalize(s2)

    list_s1 =  [word for word in s1.split(" ") if word not in hazm.stopwords_list()]
    list_s2 = [word for word in s2.split(" ") if word not in hazm.stopwords_list()]

    stemmer = hazm.Stemmer()
    stem_s1 = [stemmer.stem(word) for word in list_s1]

    same_words = set.intersection(set(list_s1), set(list_s2))
    return len(same_words)


def get_ambiguaty_abstract():
    abstract_filename = os.listdir(Config.extracted_texts_dir)
    input_ambiguate_word_filename = join(Config.article_names_dir, Config.farsnet_ambiguate_word_filename)
    output_ambiguate_abstract_filename = join(Config.article_names_dir, Config.farsnet_ambiguate_abstract_filename)

    temp_list = []
    count = 0
    max_number = 0
    min_number = 1000
    normalizer = hazm.Normalizer()
    with open(output_ambiguate_abstract_filename, 'w') as output_file:
        csv_writer = csv.writer(output_file)
        for filename in abstract_filename:
            # if count == 1:
            #     break;
            count += 1
            print('file ' + str(count)+' is runing ' + filename)
            dict_abstract = DataUtils.load_json(Config.extracted_texts_dir, filename)
            for abstract_item in dict_abstract:
                with open(input_ambiguate_word_filename, 'r') as ambiguate_word:
                    csv_reader = csv.reader(ambiguate_word)

                    for line in csv_reader:
                        item = normalizer.normalize(line[1])
                        if item == abstract_item:
                            print('find '+line[1]+' in file.')
                            del temp_list[:]
                            temp_list.append(line[0])
                            temp_list.append(normalizer.normalize(line[1]))
                            temp_list.append(line[2])
                            temp_list.append(normalizer.normalize(line[3]))
                            temp_list.append(normalizer.normalize(line[4]))
                            temp_list.append(normalizer.normalize(line[5]))
                            temp_list.append(normalizer.normalize(dict_abstract[abstract_item]))

                            sentence_snapshot = str(line[3]).replace(',', ' ').replace('،', ' ') + ' '
                            gloss_sentence = str(line[4]).replace(',', ' ').replace('،', ' ') + ' '
                            example = gloss = str(line[5]).replace(',', ' ').replace('،', ' ') + ' '
                            sentence1 = sentence_snapshot + gloss_sentence + example
                            sentence2 = str(temp_list[6]).replace(',', ' ').replace('،', ' ').replace('.', ' ')

                            diff = similar(sentence1, sentence2)
                            if diff > max_number:
                                max_number = diff
                            if diff < min_number:
                                min_number = diff
                            temp_list.append(diff)
                            csv_writer.writerow(temp_list)

    return [max_number, min_number]


#find pages which are disambiguate in wikipedia
def find_farsnet_disambiguate_page():
    input_ambiguate_abstract_filename = join(Config.article_names_dir, Config.farsnet_ambiguate_abstract_filename)
    disambiguate_filename = os.listdir(Config.extracted_disambiguations_dir)
    abstract_filename = os.listdir(Config.extracted_texts_dir)
    output_disambiguate_wiki = join(Config.article_names_dir, Config.farsnet_disambiguate_wiki_filename)

    max_number = 0
    min_number = 1000
    with open(output_disambiguate_wiki, 'w') as output_file, open(input_ambiguate_abstract_filename, 'r') as input_file:
        csv_writer, csv_reader = csv.writer(output_file), csv.reader(input_file)

        for line in csv_reader:
            for disambiguate_file in disambiguate_filename:
                list_disambiguate = DataUtils.load_json(Config.extracted_disambiguations_dir, disambiguate_file)
                # for item in list_disambiguate:
                #     if line[1] ==item:
                for item_disambiguate in list_disambiguate:
                    if line[1] == item_disambiguate['title']:
                        print(line[1] + ' find in disambiguate page.')

                        for abstract_file in abstract_filename:
                            list_abstract = DataUtils.load_json(Config.extracted_texts_dir, abstract_file)
                            for abstract_key in list_abstract:

                                if any(abstract_key == d for d in item_disambiguate['field']):

                                    print('find abstract_key: ' + abstract_key)
                                    sentence_snapshot = str(line[3]).replace(',', ' ').replace('،', ' ') + ' '
                                    gloss_sentence = str(line[4]).replace(',', ' ').replace('،', ' ') + ' '
                                    example = gloss = str(line[5]).replace(',', ' ').replace('،', ' ') + ' '
                                    sentence1 = sentence_snapshot + gloss_sentence + example
                                    sentence2 = str(list_abstract[abstract_key]).replace(',', ' ').replace('،',
                                                                                                           ' ').replace(
                                        '.', ' ')

                                    diff = similar(sentence1, sentence2)
                                    if diff > max_number:
                                        max_number = diff
                                    if diff < min_number:
                                        min_number = diff
                                    csv_writer.writerow(
                                        [line[0], line[1], line[2], line[3], line[4], line[5], abstract_key,
                                         list_abstract[abstract_key], diff])


def disambiguate_farsenet(max_number = 1, min_number = 1):
    input_ambiguate_abstract_filename = DataUtils.join(Config.article_names_dir,
                                                       Config.farsnet_ambiguate_abstract_filename)
    output_disambiguate_abstract_filename = DataUtils.join(Config.article_names_dir, Config.farsnet_disambiguate_score)

    with open(input_ambiguate_abstract_filename, 'r') as input_file, \
            open(output_disambiguate_abstract_filename, 'w') as output_file:
        csv_reader, csv_writer = csv.reader(input_file), csv.writer(output_file)
        for line in csv_reader:

            temp_list = []
            diff = ((float(line[7]) - min_number)/(max_number - min_number)) * 10
            csv_writer.writerow([line[0], line[1], line[2], line[3], line[4], line[5], line[6], round(diff, 2)])


def refinement_for_update():
    build_infobox_tuples()
    build_category_tuples()
