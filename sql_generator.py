import pymysql
from os.path import join, exists


class ConnectionString:
    # host = '194.225.227.161'
    # port = 3306
    # user = 'leila_oskouie'
    # password = '123456'
    # db = 'knowledge_graph'
    # use_unicode = True
    # charset = "utf8"

    host = 'localhost'
    port = 3306
    user = 'root'
    password = ''
    db = 'kg'
    use_unicode = True
    charset = "utf8"


class SqlForeignKeyStructure(object):
    def __init__(self, column, reference_table, reference_column):
        self.column = column
        self.reference_table = reference_table
        self.reference_column = reference_column


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
    except Exception as e:
        return None


def sql_create_table_command_generator(table_name, columns, primary_key=None, foreign_key=None, unique_key=None):
    """
    :param table_name:
    :param columns:
    :param primary_key: list of primary keys [key1, key2, ...]
    :param foreign_key: list of dict [{'column':'col_name','reference_table':'table_name', 'reference_column':'col_name'},{}]
    :param unique_key: defaultdict(list) {unique_name1: [col1, col2], unique_name2:[col1, col4]}
    :return:
    """

    command = "DROP TABLE IF EXISTS `%s`;\n" % table_name
    command += "CREATE TABLE `%s` (\n " % table_name

    for key, value in columns.items():
        command += "`%s` %s,\n" % (key, value)
    if primary_key:
        command += "PRIMARY KEY("
        for p_key in primary_key:
            command += "`%s`," %p_key
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

    command = command[:-2] + ')CHARSET=utf8;\n'
    return command


def insert_command(table_name, insert_dictionary_columns, rows):

    columns = ""
    for name in insert_dictionary_columns:
        columns += name+','
    columns = columns[:-1]

    command = "INSERT INTO `%s` (%s) VALUES " % (table_name, columns)

    for records in rows:
        command += "( "

        for column_name in insert_dictionary_columns:
            if insert_dictionary_columns[column_name] == 'varchar':
                command += " '%s', " % records[column_name]
            elif insert_dictionary_columns[column_name] == 'int':
                command += " %s, " % records[column_name]

        command = command[:-2] + '),'

    command = command[:-1] + ";"

    return command


def execute_command_mysql(command, message):
    conn = db_connection()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute(command)
            conn.commit()

        except Exception as e:
            print('Some Exception Occur ' + str(e))
            if message:
                print('\n ' + message)
        finally:
            conn.close()


def create_sql_dump(command, dir_path, file_name):
    file_path = join(dir_path, file_name + '.sql')
    with open(file_path, 'a') as f:
        f.write(command)


def select_mysql(command):
    try:

        conn = pymysql.connect(host=ConnectionString.host, port=ConnectionString.port, user=ConnectionString.user,
                               passwd=ConnectionString.passwd,
                               db=ConnectionString.db, use_unicode=ConnectionString.use_unicode,
                               charset=ConnectionString.charset)

        cur = conn.cursor(pymysql.cursors.DictCursor)
        result = cur.execute(command)

        return result

    except Exception as e:
        print('some exception occur ' + str(e))

    finally:
        conn.close()


def select_result_mysql(command):
    try:
        conn = pymysql.connect(host=ConnectionString.host, port=ConnectionString.port, user=ConnectionString.user, passwd=ConnectionString.passwd,
                               db=ConnectionString.db, use_unicode=ConnectionString.use_unicode, charset=ConnectionString.charset)

        cur = conn.cursor(pymysql.cursors.DictCursor)
        result = cur.execute(command)

        return cur
    except Exception as e:
        print('some exception occur ' + str(e))
    finally:
        conn.close()


def test():
    key_order = ['id', 'template_name_fa', 'template_name_en', 'approved', 'col1', 'col2', 'col3']
    table_structure = {
        'id': 'int NOT NULL AUTO_INCREMENT',
        'template_name_fa': 'varchar(500)',
        'template_name_en': 'varchar(500)',
        'approved': 'tinyint default NULL',
        'col1': 'int',
        'col2': 'int',
        'col3': 'int'
    }
    table_name = 'test'
    primary_keys = ['id', 'col1']
    foreign_keys = [SqlForeignKeyStructure('col2', 'test2', 'id2'), SqlForeignKeyStructure('col3', 'test3', 'id3')]
    unique_keys = {'col1_uniq': ['col1', 'col2'], 'col3_uniq': ['col2', 'col3']}
    print(sql_create_table_command_generator(table_name, table_structure, primary_keys, foreign_keys, unique_keys))


if __name__ == '__main__':
    test()