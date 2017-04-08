import json
import operator
import os
import re

import Config
import DataUtils


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
        r'(?:/?|[/?]\S+)$)', re.IGNORECASE)

    pic_prefix = ['jpg', 'tif', 'tiff', 'gif', 'png', 'jpeg', 'svg', 'exif',
                  'bmp', 'ppm', 'pgm', 'pbm', 'pnm', 'webp', 'heif', 'bat']

    for myKey in my_dict:

        my_value = my_dict[myKey]
        if isinstance(my_value, dict):
            check_path(my_value, path_names, page_n)
        else:
            result = regex.match(my_value)
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

    pic_prefix = ['jpg', 'tif', 'tiff', 'gif', 'png', 'jpeg', 'svg', 'exif',
                  'bmp', 'ppm', 'pgm', 'pbm', 'pnm', 'webp', 'heif', 'bat']

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


def get_attribute_name(attribute_type):

    dir_path = Config.extracted_dir + '/infoboxes'
    main_list = get_file_list_from_dir(dir_path)

    attribute_names = {}

    for my_id, dstFile in enumerate(main_list):

        infobox = open(dstFile)
        data = json.load(infobox)

        for page_title, pageInfo in data.items():
            if attribute_type == 'image':
                check_image(pageInfo, attribute_names, page_title)
            else:
                check_path(pageInfo, attribute_names, page_title)

    return attribute_names


def get_image_name(filename):
    att_name = get_attribute_name('image')
    sorted_image_names = sorted(att_name.items(), key=operator.itemgetter(1), reverse=True)
    DataUtils.save_json(Config.extracted_image_name_dir, filename, sorted_image_names)


def get_path_name(filename):
    att_name = get_attribute_name('path')
    sorted_path_names = sorted(att_name.items(), key=lambda i: i[1][0], reverse=True)
    DataUtils.save_json(Config.extracted_path_name_dir, filename, sorted_path_names)


if __name__ == '__main__':
    get_image_name('image_name')
    get_path_name('path_name')
