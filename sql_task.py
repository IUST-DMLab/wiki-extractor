import os
from bs4 import BeautifulSoup
import json
from collections import OrderedDict
import Config
import Utils


def json_to_row():

    dir_path = os.path.dirname(os.path.realpath(__file__)) + '/resources/DBpedia_mapping_infobox_to_Class'
    mapping_infobox = open(dir_path)
    data = json.load(mapping_infobox)

    row = []
    for my_key in data:

        list_value = data[my_key]
        for mapping_info in list_value:
            for map_key in mapping_info:
                template_type = my_key
                if my_key == 'infobox':
                    template_type = my_key
                    template_name = map_key.lower().replace('infobox_', '')
                else:
                    template_name = map_key
                    if 'stub' in map_key.lower():
                        template_type = 'stub'
                    else:
                        template_type = 'template'

                dict_row = {}
                dict_row['template_type'] = template_type
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


def create_table_mysql_template():
    dir_path = Config.extracted_infoboxes_dir
    main_list = get_file_list_from_dir(dir_path)

    table_structure = OrderedDict([('id', 'int(10) NOT NULL AUTO_INCREMENT'), ('template_name', 'varchar(250)'),
                                   ('type', 'varchar(250)'),
                                   ('language_name', 'varchar(250)')])

    insert_columns = ['template_name', 'type', 'language_name']

    primary_key = ['id']
    command = create_table_command('wiki_farsi_templates', table_structure, primary_key)

    for my_id, dstFile in enumerate(main_list):
        my_row = json_to_row_template(dstFile)
        command += insert_command_template('wiki_farsi_templates', my_row, insert_columns)

    file_path = os.path.dirname(os.path.realpath(__file__)) + '/resources/template_info.sql'
    with open(file_path, 'w') as f:
        f.write(command)


def insert_command_template(table_name, rows, insert_columns):
    columns = ""
    for name in insert_columns:
        columns += name + ','
    columns = columns[:-1]

    command = "INSERT INTO `%s` (%s) VALUES " % (table_name, columns)
    for records in rows:
        command += "('%s','%s','%s')," % (records[insert_columns[0]], records[insert_columns[1]],
                                          records[insert_columns[2]])

    command = command[:-1] + ";"

    return command


def json_to_row_template(filename):
    # dir_path = Config.extracted_disambiguation_dir + '/'+filename
    mapping_infobox = open(filename)
    data = json.load(mapping_infobox)

    return data


def create_table_command(table_name, columns, primary_key):

    command = "DROP TABLE IF EXISTS `%s`;\n" % table_name
    command += "CREATE TABLE `%s` (\n " % table_name

    for key, value in columns.items():
        command += "`%s` %s,\n" % (key, value)

    command += "PRIMARY KEY ("
    for p_key in primary_key:
        command += "`%s`," % p_key
    command = command[:-1] + ')'

    command = command + ');\n'
    return command


def insert_command_template(table_name, rows, insert_columns):

    columns = ""
    for name in insert_columns:
        columns += name+','
    columns = columns[:-1]

    command = "INSERT INTO `%s` (%s) VALUES " % (table_name, columns)
    for records in rows:
        command += "('%s','%s','%s')," % (records[insert_columns[0]], records[insert_columns[1]],
                                          records[insert_columns[2]])

    command = command[:-1] + ";"

    return command


if __name__ == '__main__':

    create_table_mysql_template()