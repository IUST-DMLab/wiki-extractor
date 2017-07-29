import shutil
import os
from os.path import join
from http import HTTPStatus
from datetime import datetime

import wget
import feedparser

import Config
import DataUtils
import LogUtils
from refiners import build_infobox_tuples
from extractors import extract_fawiki_bz2_dump_information

WIKI_DUMPS_URL = "https://dumps.wikimedia.org/fawiki/latest/"
DUMP_NAMES = [
        # "fawiki-latest-page.sql.gz",
        # "fawiki-latest-categorylinks.sql.gz",
        # "fawiki-latest-pagelinks.sql.gz",
        "fawiki-latest-externallinks.sql.gz",
        # "fawiki-latest-pages-articles.xml.bz2",
        "fawiki-latest-image.sql.gz",
        # "fawiki-latest-redirect.sql.gz",
        # "fawiki-latest-langlinks.sql.gz"
    ]


def read_update_etags():
    try:
        update_etags = DataUtils.load_json(Config.update_dir, Config.wiki_rss_etags_filename)
    except FileNotFoundError:
        update_etags = {dump_name: '' for dump_name in DUMP_NAMES}
        DataUtils.save_json(Config.update_dir, Config.wiki_rss_etags_filename, update_etags)
    return update_etags


def build_rss_path(dump_name):
    return WIKI_DUMPS_URL + dump_name + '-rss.xml'


def convert_timestamp(timestamp):
    return datetime.strptime(timestamp, '%a, %d %b %Y %X %Z')


def download_new_dumps(dump_addresses):
    for url in dump_addresses:
        try:
            LogUtils.logging_start_wget(url)
            wget.download(url, Config.wikipedia_dumps_dir)
            LogUtils.logging_finish_wget(url)
        except Exception as e:
            print(e)
            return False
    return True


def copy_directory(source, destination):
    try:
        shutil.copytree(source, destination)
        LogUtils.logging_copy_directory(source, destination)
    except shutil.Error or OSError:
        LogUtils.logging_warning_copy_directory(source)
        return False
    return True


def copy_result(new_version_dir):
    # destination_address = join('/mnt/shared/extractedWikiInfo', new_version_dir)
    destination_address = join('/home/nasim/Projects/clean_wiki_extractor', new_version_dir)

    result_directories = [
        Config.extracted_with_infobox_dir, Config.extracted_without_infobox_dir, Config.final_tuples_dir,
        Config.extracted_revision_ids_dir, Config.extracted_redirects_dir, Config.extracted_disambiguations_dir
    ]
    for directory in result_directories:
        if not copy_directory(directory, destination_address):
            return False
    return True


def delete_directory(directory):
    shutil.rmtree(directory)


def rename_directory(source, destination):
    os.rename(source, destination)


def prepare_path():
    delete_directory(Config.previous_resources_dir)
    rename_directory(Config.resources_dir, Config.previous_resources_dir)
    DataUtils.create_directory(Config.resources_dir)


def rss_reader():
    last_etags = read_update_etags()
    new_etags = dict()

    new_update_status = dict()
    new_update_date = ''
    dump_address = list()

    for dump_name in DUMP_NAMES:
        rss_address = build_rss_path(dump_name)
        d1 = feedparser.parse(rss_address, etag=last_etags[dump_name])
        if d1.status == HTTPStatus.OK:
            new_etags[dump_name] = d1.etag
            dump_address.append(d1.entries[0].summary_detail.base)
            new_update_date = d1.modified

        elif d1.status == HTTPStatus.NOT_MODIFIED:
            pass

        elif d1.status == HTTPStatus.GONE:
            LogUtils.logging_warning_read_rss(rss_address)

        print(d1.status)
        print(d1.modified)

        new_update_status[dump_name] = d1.status

    if all(status == HTTPStatus.OK for status in new_update_status.values()):
        new_version_date = convert_timestamp(new_update_date)
        news_version_dir = str(new_version_date.year) + '_' + str(new_version_date.month) + '_' + str(
            new_version_date.day)
        prepare_path()
        if download_new_dumps(dump_address):
            DataUtils.save_json(Config.update_dir, Config.wiki_rss_etags_filename, new_etags)
            return True, news_version_dir

    return False, None


def update():
    is_updated, new_version_dir = rss_reader()
    if is_updated:
        extract_fawiki_bz2_dump_information()
        build_infobox_tuples()
        if copy_result(new_version_dir):
            pass
            # send request to urls


if __name__ == '__main__':
    rss_reader()
