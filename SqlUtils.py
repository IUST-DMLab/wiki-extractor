import csv
import gzip
from collections import OrderedDict
from os.path import join

import pymysql

import Config
import DataUtils
import LogUtils


def save_sql_dump(directory, filename, sql_dump, encoding='utf8'):
    if len(directory) < 255:
        DataUtils.create_directory(directory)
        sql_filename = join(directory, filename)
        sql_file = open(sql_filename, 'w+', encoding=encoding)
        sql_file.write(sql_dump)
        sql_file.close()


def find_sql_records(line):
    records_str = line.partition('` VALUES ')[2]
    records_str = records_str.strip()[1:-2]
    records = records_str.split('),(')
    return records


def get_sql_rows(file_name, encoding='utf8', quotechar="'"):
    with gzip.open(file_name, 'rt', encoding=encoding, errors='ignore') as f:
        LogUtils.logging_file_opening(file_name)
        for line in f:
            if line.startswith('INSERT INTO '):
                all_records = find_sql_records(line)
                for record in all_records:
                    yield csv.reader([record], delimiter=',', doublequote=False,
                                     escapechar='\\', quotechar=quotechar, strict=True)
    LogUtils.logging_file_closing(file_name)


def get_wiki_templates_transcluded_on_pages_sql_dump(rows, order):
    table_name = Config.wiki_templates_transcluded_on_pages_table_name
    command = "INSERT INTO `%s` VALUES " % table_name
    for i, row in enumerate(rows):
        command += "('%s','%s','%s',%s)," % (row[order[0]].replace("'", "''"), row[order[1]].replace("'", "''"),
                                             row[order[2]].replace("'", "''"), row[order[3]])
        if (i+1) % 100 == 0:
            command = command[:-1] + ";\nINSERT INTO `%s` VALUES " % table_name

    command = command[:-1] + ";"
    return command


def get_wiki_template_redirect_sql_dump(redirects):
    table_name = 'wiki_template_redirect'
    command = "INSERT INTO `%s` VALUES " % table_name
    for i, redirect_from in enumerate(redirects):
        redirect_to = redirects[redirect_from]
        command += "('%s','%s')," % (redirect_from.replace("'", "''"), redirect_to.replace("'", "''"))
        if (i+1) % 100 == 0:
            command = command[:-1] + ";\nINSERT INTO `%s` VALUES " % table_name

    command = command[:-1] + ";"
    return command


class SqlForeignKeyStructure(object):
    def __init__(self, column, reference_table, reference_column):
        self.column = column
        self.reference_table = reference_table
        self.reference_column = reference_column


def create_order_structure(table_structure, key_order):
    return OrderedDict(sorted(table_structure.items(), key=lambda i: key_order.index(i[0])))


def db_connection():
    try:
        connection_string = Config.ConnectionString
        connection = pymysql.connect(host=connection_string.host,
                                     port=connection_string.port,
                                     user=connection_string.user,
                                     password=connection_string.password,
                                     db=connection_string.db,
                                     use_unicode=connection_string.use_unicode,
                                     charset=connection_string.charset,
                                     )
        return connection
    except Exception as e:
        raise e


def sql_create_table_command_generator(table_name, columns, primary_key=None, foreign_key=None,
                                       unique_key=None, index=None, drop_table=False):
    """
    :param table_name:
    :param columns:
    :param primary_key: list of primary keys [key1, key2, ...]
    :param foreign_key: list of dict [{'column':'col_name','reference_table':'table_name',
                                       'reference_column':'col_name'},{}]
    :param unique_key: defaultdict(list) {unique_name1: [col1, col2], unique_name2:[col1, col4]}
    :param drop_table: boolean, set True for drop table command
    :param index: dic {'key_name': 'ref_column'}
    :return:
    """
    if drop_table:
        command = "DROP TABLE IF EXISTS `%s`;\n" % table_name
    else:
        command = ''
    command += "CREATE TABLE `%s` (\n " % table_name

    for key, value in columns.items():
        command += "`%s` %s,\n" % (key, value)
    if primary_key:
        command += "PRIMARY KEY("
        for p_key in primary_key:
            command += "`%s`," % p_key
        command = command[:-1] + "),\n"

    if foreign_key:
        for f_key in foreign_key:
            command += "FOREIGN KEY (%s) REFERENCES %s(%s),\n" % (f_key.column, f_key.reference_table,
                                                                  f_key.reference_column)
    if unique_key:
        for u_key_name, u_key_values in unique_key.items():
            command += "UNIQUE KEY `%s` (" % u_key_name
            for val in u_key_values:
                command += '`%s`,' % val
            command = command[:-1] + "),\n"

    if index:
        for index_name, index_ref in index.items():
            command += "KEY `%s` (%s),\n" % (index_name, index_ref)

    command = command[:-2] + ')CHARSET=utf8;\n'
    return command


def create_values_insert_command(rows, insert_columns, table_structure):
    command = ''
    for records in rows:
        command += "( "
        for column_name in insert_columns:
            if 'varchar' in table_structure[column_name]:
                command += " '%s', " % records[column_name].replace("'", "''").replace("\\", "\\\\")
            elif 'int' in table_structure[column_name]:
                command += " %s, " % records[column_name]
        command = command[:-2] + '),'
    command = command[:-1] + ";"
    return command


def create_header_insert_command(insert_columns, table_name):
    columns_name = '(`' + '`,`'.join(insert_columns) + '`)'
    command = "INSERT INTO `%s`%s VALUES " % (table_name, columns_name)
    return command


def insert_command(table_structure, table_name, insert_columns, rows):
    command = create_header_insert_command(insert_columns, table_name)
    command += create_values_insert_command(rows, insert_columns, table_structure)

    return command


def execute_command_mysql(command, message=None):
    conn = db_connection()
    try:
            cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(command)
            conn.commit()
            return cur

    except Exception as e:
        print('Some Exception Occurred ' + str(e))
        if message:
            print('\n ' + message)
    finally:
        conn.close()


def create_sql_dump(command, dir_path, file_name):
    file_path = join(dir_path, file_name + '.sql')
    with open(file_path, 'a') as f:
        f.write(command)
