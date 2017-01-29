import bz2
import logging
import os
from os.path import join, exists

from bs4 import BeautifulSoup

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s ', level=logging.DEBUG)


def logging_file_operations(filename, operation):
    logging.info('%s %s!' % (filename, operation))


def logging_pages_extraction(pages_number, filename):
    logging.info('%d Pages Extracted from %s!' % (pages_number, filename))


def logging_information_extraction(pages_number, filename, info):
    logging.info('%d Pages Checked from %s for %s!' % (pages_number, filename, info))


def logging_database(msg):
    logging.exception('%s Error!' % msg)


def get_information_filename(info_dir, file_number):
    return join(info_dir, str(file_number)+'.json')


def create_directory(directory):
    if not exists(directory):
        logging.info(' Create All Directories in Path %s' % directory)
        os.makedirs(directory)


def get_wikipedia_pages(filename):
    if 'bz2' not in filename:
        input_file = open(filename, 'r+')
    else:
        input_file = bz2.open(filename, mode='rt', encoding='utf8')

    logging_file_operations(filename, 'Opened')

    while True:
        l = input_file.readline()
        page = list()
        if not l:
            break
        if l.strip() == '<page>':
            page.append(l)
            while l.strip() != '</page>':
                l = input_file.readline()
                page.append(l)
        if page:
            yield '\n'.join(page)
        del page

    input_file.close()
    logging_file_operations(filename, 'Closed')


def parse_page(xml_page):
    soup = BeautifulSoup(xml_page, "xml")
    page = soup.find('page')
    return page
