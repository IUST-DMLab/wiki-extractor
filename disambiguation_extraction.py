import json
import Config
import Utils
from Utils import get_wikipedia_pages
import wikitextparser as wtp

def get_disambiguation_links(content):
    sentences = content.splitlines()

    sub_str = []
    for sentence in sentences:
        if sentence.startswith('*'):
            end_index = sentence.find(']]')
            start_index = sentence.find('[[')
            sub_str.append(sentence[start_index:(end_index + 2)])

    return str


def extract_disambiguation(filename):
    disambiguation_filename = Utils.get_information_filename(Config.extracted_disambiguation_dir, filename)

    input_filename = Config.fawiki_latest_pages_meta_current_dump
    wikipedia_pages = get_wikipedia_pages(input_filename)

    json_list = []
    for page in wikipedia_pages:

        wiki_text = page.revision.find('text').text

        parse_wiki_text = wtp.parse(str(page))

        json_dict = {}

        if any('ابهام‌زدایی' in s.name for s in parse_wiki_text.templates):
            json_dict['title'] = page.title.text
            json_dict['field'] = get_disambiguation_links(str(page.contents))
            json_list.append(json_dict)

    disambiguation_file = open(disambiguation_filename, 'w+', encoding='utf8')
    disambiguation_file.write(json.dumps(json_list, ensure_ascii=False, indent=2, sort_keys=True))
    disambiguation_file.close()