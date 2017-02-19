import json
import Config
import os
import Utils
import re
from Utils import get_wikipedia_pages
from Utils import parse_page
import wikitextparser as wtp
import operator


def get_disambiguation_links(content,exception_list):

    sentences = content.splitlines()

    sub_str = []
    exception_list =[]
    for sentence in sentences:
        if sentence.startswith('*'):
            end_index = sentence.find(']]')
            start_index = sentence.find('[[')
            if end_index != -1 and start_index != -1 and start_index < end_index:
                sub_str.append(sentence[start_index:(end_index + 2)])
            elif end_index != -1 and start_index != -1:
                exception_list.append(sentence)
                print('there is an exception maybe it is conflict with disambiguation law./n sentence:'+sentence)

    return sub_str


def extract_disambiguation(filename):
    disambiguation_filename = Utils.get_information_filename(Config.extracted_disambiguation_dir, filename)

    input_filename = Config.fawiki_latest_pages_meta_current_dump
    wikipedia_pages = get_wikipedia_pages(input_filename)

    json_list = []
    exception_list = []
    #count = 0
    for page in get_wikipedia_pages(input_filename):

        # if count == 50:
        #     break
        parsed_page = parse_page(page)
        wiki_text = parsed_page.revision.find('text').text

        parse_wiki_text = wtp.parse(str(parsed_page))

        json_dict = {}

        disambiguation_name = ['ابهام‌زدایی', 'ابهام زدایی']
        for names in disambiguation_name:

            if any(names in s.name for s in parse_wiki_text.templates):
                # count = count + 1
                # if 'پهلوی' in parsed_page.title.text:
                #     print('پهلوی')
                json_dict['title'] = parsed_page.title.text
                json_dict['field'] = get_disambiguation_links(str(parsed_page.contents),exception_list)
                json_list.append(json_dict)

    disambiguation_file = open(disambiguation_filename, 'w+', encoding='utf8')
    disambiguation_file.write(json.dumps(json_list, ensure_ascii=False, indent=2, sort_keys=True))
    disambiguation_file.close()


def get_file_list_from_dir(dir_name):

    file_list = []
    for root, dirs, file_name in os.walk(dir_name):
        for filename in file_name:
            real_path = os.path.join(root, filename)
            file_list.append(real_path)
    return file_list


def check_path(my_dict, path_names, page_n):

    regex = re.compile(
        r'(?:^(?:http|ftp)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$)'
        , re.IGNORECASE)

    # regex2 = re.compile(
    #     r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    #     r'localhost|'  # localhost...
    #     r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    #     r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    pic_prefix = ['jpg', 'tif', 'tiff', 'gif', 'png', 'jpeg', 'svg', 'exif', 'bmp', 'ppm','pgm', 'pbm', 'pnm', 'webp', 'heif', 'bat']

    for myKey in my_dict:

        my_value = my_dict[myKey]
        if isinstance(my_value, dict):
            check_path(my_value, path_names, page_n)
        else:
            result = regex.match(my_value)
            #be aware
            result2 = regex.match(my_value)
            if not(str(result) == 'None'):
                if not (any(s in my_value.lower() for s in pic_prefix)):
                    if myKey in path_names.keys():
                        count = path_names[myKey][0]
                        my_word = [(count + 1), my_value]
                        path_names[myKey] = my_word

                    else:
                        my_word = [1, my_value]
                        path_names[myKey] = my_word


def check_image(my_dict, image_names, page_n):

    pic_prefix = ['jpg', 'tif','tiff', 'gif', 'png', 'jpeg', 'svg', 'exif', 'bmp', 'ppm','pgm', 'pbm', 'pnm', 'webp', 'heif', 'bat']

    for my_Key in my_dict:
        my_value = my_dict[my_Key]
        if isinstance(my_value, dict):
            check_image(my_value, image_names, page_n)
        else:

            for s in pic_prefix:
                if s in my_value.lower():

                    if my_Key in image_names.keys():
                        count = image_names[my_Key]
                        image_names[my_Key] = count + 1
                    else:
                        image_names[my_Key] = 1
                    break


def get_attribute_name(filename, attribute_type):

    dir_path = Config.extracted_infoboxes_dir
    main_list = get_file_list_from_dir(dir_path)

    attribute_names = {}

    count = 0
    for my_id,dstFile in enumerate(main_list):

        # count = count + 1
        # if count == 4:
        #     break

        infobox = open(dstFile)
        data = json.load(infobox)

        for page_title, pageInfo in data.items():
            if attribute_type == 'image':
                check_image(pageInfo, attribute_names, page_title)
            else:
                check_path(pageInfo, attribute_names, page_title)

    return attribute_names


def get_image_name(filename):

    att_name = get_attribute_name(filename, 'image')

    image_filename = Utils.get_information_filename(Config.extracted_disambiguation_dir, filename)
    image_name_file = open(image_filename, 'a+', encoding='utf8')
    sorted_image_names = sorted(att_name.items(), key=operator.itemgetter(1), reverse=True)
    image_name_file.write(json.dumps(sorted_image_names, ensure_ascii=False, indent=2, sort_keys=True))

    image_name_file.close()


def get_path_name(filename):

    att_name = get_attribute_name(filename, 'path')

    path_filename = Utils.get_information_filename(Config.extracted_disambiguation_dir, filename)
    path_name_file = open(path_filename, 'a+', encoding='utf8')

    sorted_path_names = sorted(att_name.items(), key=lambda i: i[1][0], reverse=True)
    path_name_file.write(json.dumps(sorted_path_names, ensure_ascii=False, indent=2, sort_keys=True))

    path_name_file.close()

if __name__ == '__main__':
    get_path_name('path1')
    #extract_disambiguation('disambiguation')
