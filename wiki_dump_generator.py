import logging
from os.path import join
from xml.dom import minidom
import xml.etree.ElementTree as Et

import requests

import Config


class MediaWiki(object):
    def __init__(self, url):
        logging.getLogger().setLevel(logging.INFO)
        self.wiki_url = url if url.startswith('http://') else 'http://' + url
        self.api = self.wiki_url + '/api.php?'
        self.dump_filename = self.wiki_url[7:self.wiki_url.rfind('.')].replace('.', '_') + '.xml'
        self.dump_path = join(Config.generated_dumps_dir, self.dump_filename)
        self.root = Et.Element('mediawiki')
        self.tree = Et.ElementTree(self.root)

    def get_pages_list_query(self):
        params = dict()
        params['action'] = 'query'
        params['format'] = 'json'
        params['generator'] = 'allpages'
        params['continue'] = None
        params['gaplimit'] = 50
        last_continue = {}

        while True:
            params.update(last_continue)
            result = requests.get(self.api, params=params).json()
            if 'error' in result:
                logging.error(result['error'])
            if 'warnings' in result:
                logging.warning(result['warnings'])
            if 'query' in result:
                yield result['query']
            if 'continue' not in result:
                break
            last_continue = result['continue']

    def get_page_query(self, title):
        params = dict()
        params['action'] = 'query'
        params['format'] = 'json'
        params['prop'] = 'revisions'
        params['rvprop'] = 'content|ids'
        params['titles'] = title

        result = requests.get(self.api, params=params).json()
        if 'error' in result:
            logging.error(result['error'])
        if 'warnings' in result:
            logging.warning(result['warnings'])
        if 'query' in result:
            return result['query']

    def _prepare_title_from_pages_list(self, result):
        titles = list()
        for page in result['pages'].values():
            titles.append(page['title'])
        return '|'.join(titles)

    def write_pretty_xml(self):
        xml = minidom.parseString(Et.tostring(self.root)).toprettyxml(indent="   ")
        with open(self.dump_path, 'w') as output:
            output.write(xml)

    def generate_dump(self):
        logging.info("generating %s dump ..." % self.wiki_url)
        for pages_list in self.get_pages_list_query():
            pages = pages_list['pages']
            pages_body = self.get_page_query(self._prepare_title_from_pages_list(pages_list))['pages']
            for page_id in pages:
                page = Et.SubElement(self.root, 'page')

                title = Et.SubElement(page, 'title')
                title.text = pages[page_id]['title']

                ns = Et.SubElement(page, 'ns')
                ns.text = str(pages[page_id]['ns'])

                p_id = Et.SubElement(page, 'id')
                p_id.text = str(pages[page_id]['pageid'])

                revision = Et.SubElement(page, 'revision')
                page_revision = pages_body[page_id]['revisions'][0]

                revision_id = Et.SubElement(revision, 'id')
                revision_id.text = str(page_revision['revid'])

                parent_id = Et.SubElement(revision, 'parentid')
                parent_id.text = str(page_revision['parentid'])

                model = Et.SubElement(revision, 'model')
                model.text = page_revision['contentmodel']

                content_format = Et.SubElement(revision, 'format')
                content_format.text = page_revision['contentformat']

                wiki_text = Et.SubElement(revision, 'text')
                wiki_text.text = page_revision['*']

        self.write_pretty_xml()


def build_dump():
    wiki_urls = ['http://fa.wikishia.net', 'http://wiki.ahlolbait.com', 'http://wiki.linuxreview.ir',
                 'http://wiki.fantasy.ir', 'http://wiki.islamicdoc.org']
    wiki_objects = [MediaWiki(url) for url in wiki_urls]
    for obj in wiki_objects:
        obj.generate_dump()
