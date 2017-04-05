import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s ', level=logging.DEBUG)


def logging_file_operations(filename, operation):
    logging.info('%s %s!' % (filename, operation))


def logging_file_opening(filename):
    logging_file_operations(filename, 'Opened')


def logging_file_closing(filename):
    logging_file_operations(filename, 'Closed')


def logging_extraction(pages_number, filename, message):
    logging.info('%d Pages %s from %s!' % (pages_number, message, filename))


def logging_pages_extraction(pages_number, filename):
    logging_extraction(pages_number, filename, 'Extracted')


def logging_information_extraction(pages_number, filename):
    logging_extraction(pages_number, filename, 'Checked')
