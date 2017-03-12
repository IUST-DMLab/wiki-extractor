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

# general mysql


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


def sql_create_table_command(table_name, columns, index):

    command = "DROP TABLE IF EXISTS `%s`;\n" % table_name
    command += "CREATE TABLE `%s` (\n " % table_name

    for key, value in columns.items():
        command += "`%s` %s,\n" % (key, value)

    command += index
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
    try:
        conn = pymysql.connect(host=ConnectionString.host, port=ConnectionString.port, user=ConnectionString.user,
                               passwd=ConnectionString.passwd,
                               db=ConnectionString.db, use_unicode=ConnectionString.use_unicode,
                               charset=ConnectionString.charset)
        cur = conn.cursor()
        cur.execute(command)
        conn.commit()

    except Exception as e:
        print('Some Exception Occur ' + str(e))
        if not message:
            print('\n ' + message)
    finally:
        conn.close()


def create_import_file(command, dir_path, file_name):

    file_path = dir_path = join(dir_path, file_name + '.sql')
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

