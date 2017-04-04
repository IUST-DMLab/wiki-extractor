import json
import os
from collections import OrderedDict

import Config
import SqlUtils


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


def get_file_list_from_dir(dir_name):

    file_list = []
    for root, dirs, file_name in os.walk(dir_name):
        for filename in file_name:
            real_path = os.path.join(root, filename)
            file_list.append(real_path)
    return file_list


if __name__ == '__main__':
    create_table_mysql_template('wiki_en_templates', Config.extracted_en_template_names_dir)
