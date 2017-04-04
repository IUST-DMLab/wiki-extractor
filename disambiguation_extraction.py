import Config
import Utils
import re
from Utils import parse_page
import wikitextparser as wtp


def get_disambiguation_links_regular(content):

    sentences = content.splitlines()
    sub_str = []

    for sentence in sentences:
        if 'جستارهای وابسته' in sentence:
            break

        regex = re.compile(r'(?:^\* *\[\[.+?\]\])')
        result = regex.match(sentence)
        if not (str(result) == 'None'):
            end_index = sentence.find(']]')
            start_index = sentence.find('[[')
            sub_str.append(sentence[start_index:(end_index + 2)])

    return sub_str


def extract_disambiguation(filename, dir_name):

    input_filename = dir_name
    json_list = []

    for page in Utils.get_wikipedia_pages(input_filename):

        parsed_page = parse_page(page)
        parse_wiki_text = wtp.parse(str(parsed_page))

        json_dict = {}

        disambiguation_name = Config.disambigution_flags
        for names in disambiguation_name:

            if any(names in s.name for s in parse_wiki_text.templates):
                json_dict['title'] = parsed_page.title.text
                json_dict['field'] = get_disambiguation_links_regular(str(parsed_page.contents))
                json_list.append(json_dict)

    Utils.save_json(Config.extracted_disambiguation_dir, filename, json_list)


if __name__ == '__main__':
    extract_disambiguation('disambiguation')
