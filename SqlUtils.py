import csv
import gzip
from os.path import join

from Utils import create_directory, logging_file_opening, logging_file_closing


def save_sql_dump(directory, filename, sql_dump, encoding='utf8'):
    if len(directory) < 255:
        create_directory(directory)
        sql_filename = join(directory, filename)
        sql_file = open(sql_filename, 'w+', encoding=encoding)
        sql_file.write(sql_dump)
        sql_file.close()


def find_sql_records(line):
    records_str = line.partition('` VALUES ')[2]
    records_str = records_str.strip()[1:-2]
    records = records_str.split('),(')
    return records


def get_sql_rows(file_name, encoding='utf8'):
    with gzip.open(file_name, 'rt', encoding=encoding, errors='ignore') as f:
        logging_file_opening(file_name)
        for line in f:
            if line.startswith('INSERT INTO '):
                all_records = find_sql_records(line)
                for record in all_records:
                    yield csv.reader([record], delimiter=',', doublequote=False,
                                     escapechar='\\', quotechar="'", strict=True)
    logging_file_closing(file_name)


def get_wiki_templates_transcluded_on_pages_sql_dump(rows, order):
    table_name = 'wiki_templates_transcluded_on_pages'
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
