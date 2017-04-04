import os
import json
from collections import OrderedDict
import Config
import Utils
from ThirdParty.WikiCleaner import clean
from os.path import join, exists
import sql_generator


def get_template():

    dir_path = Config.resources_dir + '/all'
    file_map = open(dir_path, 'r', encoding='utf8')

    start_temp = 0
    template = []

    for line in file_map:

        if '@prefix' in line:
            start_temp = 0
        elif start_temp == 0:
            start_temp = 1
            yield template
            template = []
        else:
            template.append(line)


def entity_map(file_name):

    list_map = []
    other_map = []
    key_list_map = []
    key_other_map = []

    entity_indicator = 'mappings/en/'

    for tmp in get_template():
        if len(tmp) != 0:

            str_tmp = tmp[0]
            start_index = str_tmp.find('<')
            end_index = str_tmp.find('>')
            item = str_tmp[start_index:end_index]
            entity_index = str_tmp[start_index:end_index].find(entity_indicator)
            att_name = item[entity_index+len(entity_indicator):]

            if 'rr:subjectMap' in tmp[2]:
                start_index = tmp[2].find('<')
                end_index = tmp[2].find('>')
                item = tmp[2][start_index:end_index].split('/')

                dict_map = {}
                class_map_name = item[len(item) - 1]
                dict_map[att_name.lower()] = class_map_name

                if 'infobox' in att_name.lower() and not(att_name.lower() in key_list_map):
                    key_list_map.append(att_name.lower())
                    list_map.append(dict_map)

                elif not('infobox' in att_name.lower()) and not(att_name.lower() in key_other_map):
                    key_other_map.append(att_name.lower())
                    other_map.append(dict_map)

            else:

                print('there is an exception for detecting mapping class name.(there is no rr:subjectMap for template)')

    dict_final = {'infobox': list_map, 'other type': other_map}

    Utils.save_json(Config.extracted_ontology_dir, file_name, dict_final)


def dbpedia_mapping_all_templates_to_class(file_name):

    entity_indicator = 'mappings/en/'
    reverse_dict = {}

    for tmp in get_template():
        if len(tmp) != 0:
            str_tmp = tmp[0]
            start_index = str_tmp.find('<')
            end_index = str_tmp.find('>')
            item = str_tmp[start_index:end_index]
            entity_index = str_tmp[start_index:end_index].find(entity_indicator)
            att_name = item[entity_index + len(entity_indicator):]

            if 'rr:subjectMap' in tmp[2]:
                start_index = tmp[2].find('<')
                end_index = tmp[2].find('>')
                item = tmp[2][start_index:end_index].split('/')

                dict_map = {}
                class_map_name = item[len(item) - 1]
                dict_map[att_name] = class_map_name

                if not(class_map_name in reverse_dict):
                    dict_info = {}
                    dict_info['infobox'] = []
                    dict_info['stub'] = []
                    dict_info['template'] = []
                    reverse_dict[class_map_name] = dict_info

                if 'infobox' in att_name.lower():
                    reverse_dict[class_map_name]['infobox'].append(att_name)

                elif 'stub' in att_name.lower():
                    reverse_dict[class_map_name]['stub'].append(att_name)

                else:
                    reverse_dict[class_map_name]['template'].append(att_name)

    Utils.save_json(Config.extracted_ontology_dir, file_name, reverse_dict)


def create_table_mysql_dbpedia_mapping_template_to_classes(filename):

    table_structure = OrderedDict([('id', 'int(10) NOT NULL AUTO_INCREMENT'), ('type', 'varchar(250)'),
                                   ('template_name', 'varchar(250)'),
                                   ('class_name', 'varchar(250)')])

    insert_columns = ['type', 'template_name', 'class_name']
    primary_key = ['id']
    command = sql_generator.sql_create_table_command_generator('dbpedia_mapping_template_to_classes', table_structure, primary_key)
    my_row = json_to_row(filename)
    command += sql_generator.insert_command(table_structure, 'dbpedia_mapping_template_to_classes', insert_columns, my_row)

    sql_generator.execute_command_mysql(command)

    file_path = Config.resources_dir + '/dbpedia_mapping_template_to_classes.sql'
    with open(file_path, 'w') as f:
        f.write(command)


# final function
def dbpedia_mapping_template_to_classes():

    filename = 'DBpedia_mapping_Alltemplates_to_class2'
    dbpedia_mapping_all_templates_to_class(filename)

    filename = 'DBpedia_mapping_infobox_to_Class2'
    entity_map(filename)
    create_table_mysql_dbpedia_mapping_template_to_classes(filename)


def json_to_row(filename):

    dir_path = join(Config.extracted_ontology_dir, filename+'.json')
    mapping_infobox = open(dir_path)
    data = json.load(mapping_infobox)

    row = []
    for my_key in data:

        list_value = data[my_key]
        for mapping_info in list_value:
            for map_key in mapping_info:
                template_type = my_key

                template_name = map_key.lower().replace('_', ' ')
                template_type = my_key

                if 'infobox' in map_key.lower():
                    template_type = 'infobox'
                elif 'stub' in map_key.lower():
                    template_type = 'stub'
                else:
                    template_type = 'template'

                dict_row = {}
                dict_row['type'] = template_type
                dict_row['template_name'] = template_name
                dict_row['class_name'] = mapping_info[map_key]

                row.append(dict_row)

    return row


def get_file_list_from_dir(dir_name):

    file_list = []
    for root, dirs, file_name in os.walk(dir_name):
        for filename in file_name:
            real_path = os.path.join(root, filename)
            file_list.append(real_path)
    return file_list


# for wiki_template
def create_table_mysql_template(table_name, dir_path):

    main_list = get_file_list_from_dir(dir_path)

    table_structure = OrderedDict([('id', 'int(10) NOT NULL AUTO_INCREMENT'), ('template_name', 'varchar(250)'),
                                   ('type', 'varchar(250)'), ('language_name', 'varchar(250)')])

    insert_columns = ['template_name', 'type', 'language_name']

    primary_key = ['id']

    count = 0

    command = sql_generator.sql_create_table_command_generator(table_name, table_structure, primary_key)

    message = 'some exception occur while insert table wiki_farsi_templates'
    sql_generator.execute_command_mysql(command, message)

    command = ""
    for my_id, dstFile in enumerate(main_list):

        count += 1
        message = 'some exception occur in file ' + str(count)
        print('file number '+dstFile+' write \n')
        my_row = json_to_row_template(dstFile)

        command += sql_generator.insert_command(table_structure, table_name, insert_columns, my_row)
        sql_generator.execute_command_mysql(command, message)

        command = ""


def json_to_row_template(filename):

    # dir_path = Config.extracted_disambiguation_dir + '/'+filename
    mapping_infobox = open(filename)
    data = json.load(mapping_infobox)

    return data


# old version
def check_duplicate_template(table_name, template_name):

    condition = "template_name = '%s'" % template_name
    command = "select 1 from %s where %s" % (table_name, condition)

    result = sql_generator.execute_command_mysql(command)

    if result.rowcount != 0:
        return True
    else:
        return False


def update_language_wiki_template(table_name):
    command = "select * from %s" % table_name
    result = sql_generator.execute_command_mysql(command)

    for row in result:
        lang = Utils.detect_language_v2(row['template_name'])
        if lang != row['language_name']:
            command = "UPDATE %s SET language_name = '%s' WHERE id = %s" % (table_name, lang, row['id'])
            sql_generator.execute_command_mysql(command, '')


# this is temporary function
def get_infobox_type(template_name):

    template_name = clean(str(template_name).lower().replace('_', ' '))
    for name in Config.infobox_flags_en:
        if name in template_name:
            t_type = name
            return t_type

    for name in Config.infobox_flags_fa:
        if name in template_name:
            t_type = name
            return t_type

    for name in Config.stub_flag_en:
        if name in template_name:
            t_type = name
            return t_type

    for name in Config.stub_flag_fa:
        if name in template_name:
            t_type = 'خرد'
            return t_type

    return 'template'


def update_wiki_farsi_template_type(table_name):

    command = 'select * from ' + table_name
    result = sql_generator.execute_command_mysql(command)

    for row in result:
        t_type = get_infobox_type(row['template_name'])
        command = "UPDATE %s SET type = '%s' WHERE id = %s" % (table_name,t_type, row['id'])
        sql_generator.execute_command_mysql(command, '')


if __name__ == '__main__':

    #update_language_wiki_template('wiki_farsi_templates_v2')
    create_table_mysql_template('test_wiki_en_templates', Config.extracted_en_template_names_dir)
    #dbpedia_mapping_template_to_classes()
    #update_wiki_farsi_template_type('test_wiki_en_templates')










