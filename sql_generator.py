from os.path import join
from collections import OrderedDict

import pymysql


class ConnectionString:
    host = '194.225.227.161'
    port = 3306
    user = ''
    password = ''
    db = 'knowledge_graph'
    use_unicode = True
    charset = "utf8"


class SqlForeignKeyStructure(object):
    def __init__(self, column, reference_table, reference_column):
        self.column = column
        self.reference_table = reference_table
        self.reference_column = reference_column


def create_order_structure(table_structure, key_order):
    return OrderedDict(sorted(table_structure.items(), key=lambda i: key_order.index(i[0])))


def db_connection():
    try:
        connection = pymysql.connect(host=ConnectionString.host,
                                     port=ConnectionString.port,
                                     user=ConnectionString.user,
                                     password=ConnectionString.password,
                                     db=ConnectionString.db,
                                     use_unicode=ConnectionString.use_unicode,
                                     charset=ConnectionString.charset,
                                     )
        return connection
    except Exception:
        return None


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
            command += "FOREIGN KEY (%s) REFERENCE %s(%s),\n" % (f_key.column, f_key.reference_table,
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
    if conn:
        try:
            cur = conn.cursor()
            res = cur.execute(command)
            conn.commit()
            return res

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
