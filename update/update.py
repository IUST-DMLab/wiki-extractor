import time
import logging
from os.path import join
from http import HTTPStatus
from datetime import datetime

import wget
import feedparser

import Config
import DataUtils
import LogUtils
from refiners import refinement_for_update
from extractors import extraction_for_update

WIKI_DUMPS_URL = "https://dumps.wikimedia.org/fawiki/latest/"
DUMP_NAMES = [
        "fawiki-latest-page.sql.gz",
        "fawiki-latest-categorylinks.sql.gz",
        "fawiki-latest-pagelinks.sql.gz",
        "fawiki-latest-externallinks.sql.gz",
        "fawiki-latest-pages-articles.xml.bz2",
        "fawiki-latest-image.sql.gz",
        "fawiki-latest-redirect.sql.gz",
        "fawiki-latest-langlinks.sql.gz"
    ]

RESULT_DIRECTORIES = [
        Config.extracted_with_infobox_dir, Config.extracted_without_infobox_dir, Config.final_tuples_dir,
        Config.extracted_texts_dir, Config.extracted_redirects_dir, Config.extracted_disambiguations_dir,
        Config.extracted_abstracts_dir, Config.final_category_tuples_dir
    ]

DESTINATION_DIR = '/data/extractedWikiInfo'


def start_update(force_update=False):
    """":param force_update: doing update process anyway, without last update etags consideration """
    start_time = int(time.time())
    is_updated, new_version_dir = rss_reader(force_update)
    if is_updated:
        try:
            logging.info("Extraction process started.")
            extraction_for_update()
            logging.info("Extraction process finished.")

            logging.info("Refinement Process started.")
            refinement_for_update()
            logging.info("Refinement Process finished.")

            for i in range(3):
                if i:
                    logging.info("Trying copy result again ...")

                if copy_result(new_version_dir):
                    end_time = int(time.time())
                    info = {
                            "extractionStart": start_time,
                            "extractionEnd": end_time,
                            "module": "wiki",
                            }
                    DataUtils.save_json(Config.update_dir, Config.update_info_filename, info)
                    DataUtils.copy_directory(join(Config.update_dir, Config.update_info_filename),
                                             join(DESTINATION_DIR, 'last'))
                    logging.info("Successfully Updated.")
                    return True

            revert_previous_etags()
            logging.warning("Cant copy result directories, please fix the problem or copy results manually.")
            logging.info("Unsuccessful Update!")

        except Exception:
            revert_previous_etags()
            logging.warning("Unsuccessful Update!")

        return False


def rss_reader(force_update):
    last_etags = load_etags(force_update)
    new_etags = dict()

    new_update_status = dict()
    new_update_date = ''
    dump_addresses = list()

    for dump_name in DUMP_NAMES:
        rss_address = build_rss_path(dump_name)
        d = feedparser.parse(rss_address, etag=last_etags[dump_name])

        if d.status == HTTPStatus.OK:
            logging.info('%s RELEASED' % rss_address)
            dump_addresses.append(build_dump_path(dump_name))
            new_update_date = d.modified

        elif d.status == HTTPStatus.NOT_MODIFIED:
            logging.info('%s NOT MODIFIED' % rss_address)

        elif d.status == HTTPStatus.GONE:
            logging.warning('%s GONE --- Replace the alternative url ---' % rss_address)

        elif d.status == HTTPStatus.NOT_FOUND:
            logging.warning('%s NOT FOUND !' % rss_address)

        new_etags[dump_name] = d.etag
        new_update_status[dump_name] = d.status

    if all(status == HTTPStatus.OK for status in new_update_status.values()):
        logging.info("New wikipedia dump released ....")
        new_version_date = convert_timestamp(new_update_date)
        news_version_dir = make_new_version_dir_name(new_version_date)
        prepare_path()

        if download_new_dumps(dump_addresses, new_update_date):
            DataUtils.save_json(Config.update_dir, Config.wiki_rss_etags_filename, new_etags)
            return True, news_version_dir

    if any(status == HTTPStatus.OK for status in new_update_status.values()):
        logging.info("Some changes detected in wikipedia dump, not fully released yet.")
    else:
        logging.info("No changes detected in wikipedia dump.")

    return False, None


def make_new_version_dir_name(new_version_date):
    return str(new_version_date.year) + '_' + str('{:02d}'.format(new_version_date.month)) + '_' + str(
        '{:02d}'.format(new_version_date.day))


def load_etags(force_update):
    last_etags = None
    try:
        if force_update:
            etags = {dump_name: '' for dump_name in DUMP_NAMES}
        else:
            last_etags = etags = DataUtils.load_json(Config.update_dir, Config.wiki_rss_etags_filename)
            for dump in DUMP_NAMES:
                if dump not in etags:
                    etags[dump] = ''

    except FileNotFoundError:
        etags = {dump_name: '' for dump_name in DUMP_NAMES}

    if last_etags != etags:
        DataUtils.rename_file_or_directory(join(Config.update_dir, Config.wiki_rss_etags_filename),
                                           join(Config.update_dir, Config.previous_wiki_rss_etags_filename))
        DataUtils.save_json(Config.update_dir, Config.wiki_rss_etags_filename, etags)
    return etags


def copy_result(new_version_dir):
    successful_copy = True
    destination_address = join(DESTINATION_DIR, new_version_dir)
    DataUtils.create_directory(destination_address)

    for directory in RESULT_DIRECTORIES:
        if not DataUtils.copy_directory(directory, destination_address):
            successful_copy = False
    logging.info('Result directories successfully copied.')
    DataUtils.create_symlink(destination_address, join(DESTINATION_DIR, 'last'))
    return successful_copy


def download_new_dumps(dump_addresses, new_dump_version):
    dump_downloaded_version = DataUtils.load_json(Config.update_dir,
                                                  Config.download_dump_version_filename) if DataUtils.exists(
        join(Config.update_dir, Config.download_dump_version_filename)) else {'downloaded_dump_version': ''}

    if dump_downloaded_version['downloaded_dump_version'] == new_dump_version:
        logging.info("dumps read from previous download.")
        DataUtils.copy_directory(join(Config.previous_resources_dir, Config.dumps_directory_name),
                                 Config.wikipedia_dumps_dir)
    else:
        DataUtils.create_directory(Config.wikipedia_dumps_dir)
        for url in dump_addresses:
            try:
                LogUtils.logging_start_wget(url)
                wget.download(url, Config.wikipedia_dumps_dir, bar=wget.bar_thermometer)
                LogUtils.logging_finish_wget(url)
            except Exception as e:
                print(e)
                return False
        DataUtils.save_json(Config.update_dir, Config.download_dump_version_filename,
                            {'downloaded_dump_version': new_dump_version})
    return True


def build_rss_path(dump_name):
    return WIKI_DUMPS_URL + dump_name + '-rss.xml'


def build_dump_path(dump_name):
    return WIKI_DUMPS_URL + dump_name


def convert_timestamp(timestamp):
    return datetime.strptime(timestamp, '%a, %d %b %Y %X %Z')


def prepare_path():
    DataUtils.delete_directory(Config.previous_resources_dir)
    DataUtils.rename_file_or_directory(Config.resources_dir, Config.previous_resources_dir)
    DataUtils.create_directory(Config.resources_dir)


def revert_previous_etags():
    try:
        previous_etags = DataUtils.load_json(Config.update_dir, Config.previous_wiki_rss_etags_filename)
        DataUtils.save_json(Config.update_dir, Config.wiki_rss_etags_filename, previous_etags)
    except FileNotFoundError:
        DataUtils.save_json(Config.update_dir, Config.wiki_rss_etags_filename,
                            {dump_name: '' for dump_name in DUMP_NAMES})
