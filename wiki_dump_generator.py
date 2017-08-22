import logging
from os.path import join
from xml.dom import minidom
import xml.etree.ElementTree as Et

import requests

import Config


class MediaWiki(object):
    def __init__(self, url, continue_param):
        logging.getLogger().setLevel(logging.INFO)
        self.wiki_url = url if url.startswith('http://') else 'http://' + url
        self.continue_param = continue_param
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
        params[self.continue_param] = None
        params['gaplimit'] = 50  # max=500
        last_continue = {}

        while True:
            params.update(last_continue)
            print(last_continue)
            result = requests.get(self.api, params=params).json()
            if 'error' in result:
                logging.error(result['error'])
            if 'warnings' in result:
                logging.warning(result['warnings'])
            if 'query' in result:
                yield result['query']
            if self.continue_param not in result:
                break

            if self.continue_param == 'query-continue':
                last_continue = result['query-continue']['allpages']

            else:
                last_continue = result['continue']

    def get_page_query(self, ids):
        params = dict()
        params['action'] = 'query'
        params['format'] = 'json'
        params['prop'] = 'revisions'
        params['rvprop'] = 'content|ids'
        params['pageids'] = ids

        result = requests.get(self.api, params=params)
        if result.status_code == 200:
            result = result.json()
        else:
            print('get request error')
            print(ids)
            raise result.status_code
        if 'error' in result:
            logging.error(result['error'])
        if 'warnings' in result:
            logging.warning(result['warnings'])
        if 'query' in result:
            return result['query']

    def _prepare_ids_from_pages_list(self, result):
        ids = list()
        for page in result['pages'].values():
            ids.append(str(page['pageid']))
        return '|'.join(ids)

    def write_pretty_xml(self):
        xml = minidom.parseString(Et.tostring(self.root)).toprettyxml(indent="   ")
        with open(self.dump_path, 'w') as output:
            output.write(xml)

    def generate_dump(self):
        logging.info("generating %s dump ..." % self.wiki_url)
        for pages_list in self.get_pages_list_query():
            pages = pages_list['pages']
            pages_body = self.get_page_query(self._prepare_ids_from_pages_list(pages_list))['pages']
            for page_id in pages:
                page = Et.SubElement(self.root, 'page')
                try:

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
                    model.text = page_revision.get('contentmodel', '')

                    content_format = Et.SubElement(revision, 'format')
                    content_format.text = page_revision.get('contentformat', '')

                    wiki_text = Et.SubElement(revision, 'text')
                    wiki_text.text = page_revision['*']
                except KeyError:
                    self.root.remove(page)
                    print(KeyError)
                    print(page_id)
        self.write_pretty_xml()


def build_dump():
    wikis = [
        ('http://fa.wikishia.net', 'continue'),
        ('http://wiki.ahlolbait.com', 'query-continue'),
        ('http://wiki.linuxreview.ir', 'query-continue'),
        ('http://wiki.fantasy.ir', 'query-continue'),
        ('http://wiki.islamicdoc.org/wiki', 'continue')
    ]
    wiki_objects = [MediaWiki(wiki[0], wiki[1]) for wiki in wikis]
    for obj in wiki_objects:
        obj.generate_dump()
