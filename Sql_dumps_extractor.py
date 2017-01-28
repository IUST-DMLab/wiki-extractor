import json
import codecs
import Config
import pymysql
from os.path import join
from collections import defaultdict
from Utils import logging_file_operations, logging_database


def db_connection():
    try:
        connection = pymysql.connect(host=Config.MYSQL_HOST,
                                     user=Config.MYSQL_USER,
                                     password=Config.MYSQL_PASS,
                                     db=Config.MYSQL_DB,
                                     )
        return connection
    except Exception as e:
        logging_database(e)
        return None


def db_loader(connection, file_path):
    try:
        cursor = connection.cursor()
        with codecs.open(file_path, 'r', 'ISO-8859-1') as f:
            sql = f.read()
            cursor.execute(sql)
            return True

    except Exception as e:
        logging_database(e)
        return False


def dict_to_json(res, f_name):
    filename = join(Config.json_result_dir, f_name,)
    with open(filename, 'w') as fp:
        logging_file_operations(filename, 'Opened')
        json.dump(res, fp, ensure_ascii=False)
    logging_file_operations(filename, 'Closed')


def get_external_links():
    """
    dump: externallinks
    el_from --> page_id
    el_to --> url
    el_index --> search optimization
    el_index_60 --> sort
    """
    connection = db_connection()
    if connection:
        if db_loader(connection, Config.fawiki_latest_external_links_dump):
            try:
                res = defaultdict(list)
                with connection.cursor() as cursor:
                    sql = 'SELECT el_from, el_to FROM externallinks'
                    cursor.execute(sql)
                    row = cursor.fetchone()
                    while row is not None:
                        wiki_page_id = row[0]
                        url_link = row[1].decode('utf-8')
                        res[wiki_page_id].append(url_link)
                        row = cursor.fetchone()

                dict_to_json(res, 'externallinks.json')
            finally:
                connection.close()


def get_redirect():
    """
    dump: redirect
    rd_title --> target page
    rd_namespace --> target page namespace
    rd_from --> current page_id
    rd_fragment --> section of target ( title of section) 3641 rows of dump has value for this field
    rd_interwiki --> prefix of target. 72 rows of dump have value for this field
    """
    connection = db_connection()
    if connection:
        if db_loader(connection, Config.fawiki_latest_redirect_dump):
            try:
                res = defaultdict(list)
                with connection.cursor() as cursor:
                    sql = 'SELECT rd_title, rd_from From redirect'
                    cursor.execute(sql)
                    row = cursor.fetchone()
                    while row is not None:
                        target_page = row[0].decode('utf-8')
                        current_page = row[1]
                        res[current_page].append(target_page)
                        row = cursor.fetchone()

                dict_to_json(res, 'redirect.json')

            finally:
                connection.close()


def get_wiki_links():
    """
    dump:pagelinks
    pl_from --> link container page_id
    pl_from_namespace --> link container namespace
    pl_namespace --> target page namespace
    pl_title -->. target page_title
    """
    connection = db_connection()
    if connection:
        if db_loader(connection, Config.fawiki_latest_page_links_dump):
            try:
                res = defaultdict(list)
                with connection.cursor() as cursor:
                    sql = 'SELECT pl_from, pl_title From pagelinks'

                    cursor.execute(sql)
                    row = cursor.fetchone()
                    while row is not None:
                        link_container = row[0]
                        target_page = row[1].decode('utf-8')
                        res[link_container].append(target_page)
                        row = cursor.fetchone()
                dict_to_json(res, 'wikilink.json')
            finally:
                connection.close()


def get_category():
    """
    dump: categorylinks
    cl_from --> article page_id
    cl_to --> category name, without namespace , space replaced by _
    cl_collation --> what collation is in use, updatecollation.php detect changes
    """
    connection = db_connection()
    if connection:
        if db_loader(connection, Config.fawiki_latest_category_links_dump):
            try:
                res = defaultdict(list)
                with connection.cursor() as cursor:
                    sql = 'SELECT cl_from, cl_to From categorylinks'

                    cursor.execute(sql)
                    row = cursor.fetchone()
                    while row is not None:
                        link_container = row[0]
                        category = row[1].decode('utf-8')
                        res[link_container].append(category)
                        row = cursor.fetchone()

                dict_to_json(res, 'category.json')
            finally:
                connection.close()


if __name__ == '__main__':
    get_external_links()
    get_redirect()
    get_wiki_links()
    get_category()
