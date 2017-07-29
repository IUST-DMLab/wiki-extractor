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


def logging_warning_read_rss(url):
    logging.warning('requested url ("%s") is Gone!' % url)


def logging_start_wget(url):
    logging.info('wget  "%s" started' % url)


def logging_finish_wget(url):
    logging.info('wget  "%s" finished' % url)


def logging_warning_copy_directory(source):
    logging.warning('Directory %s not copied' % source)

def logging_copy_directory(source, destination):
    logging.info('Directory %s copied to %s' % source, destination)
