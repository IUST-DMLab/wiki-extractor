from os.path import join, dirname, realpath
import re

extracted_pages_per_file = dict()
extracted_pages_per_file['fa'] = 100000
extracted_pages_per_file['en'] = 1000000

logging_interval = dict()
logging_interval['fa'] = 10000
logging_interval['en'] = 100000

current_dir = dirname(realpath(__file__))
resources_dir = join(current_dir, 'resources')

wikipedia_dumps_dir = join(resources_dir, 'dumps')
extracted_dir = join(resources_dir, 'extracted')
refined_dir = join(resources_dir, 'refined')


latest_pages_articles_dump = dict()
latest_pages_articles_dump['en'] = join(wikipedia_dumps_dir, 'enwiki-latest-pages-articles.xml.bz2')
latest_pages_articles_dump['fa'] = join(wikipedia_dumps_dir, 'fawiki-latest-pages-articles.xml.bz2')

fawiki_latest_category_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-categorylinks.sql.gz')
fawiki_latest_external_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-externallinks.sql.gz')
fawiki_latest_lang_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-langlinks.sql.gz')
fawiki_latest_page_dump = join(wikipedia_dumps_dir, 'fawiki-latest-page.sql.gz')
fawiki_latest_page_links_dump = join(wikipedia_dumps_dir, 'fawiki-latest-pagelinks.sql.gz')
fawiki_latest_redirect_dump = join(wikipedia_dumps_dir, 'fawiki-latest-redirect.sql.gz')


extracted_abstracts_dir = join(extracted_dir, 'abstracts')
extracted_category_links_dir = join(extracted_dir, 'category_links')
extracted_category_links_filename = 'all'

extracted_disambiguations_dir = join(extracted_dir, 'disambiguations')

extracted_pages_articles_dir = dict()
extracted_pages_articles_dir['fa'] = join(extracted_dir, 'fa_pages_articles')
extracted_pages_articles_dir['en'] = join(extracted_dir, 'en_pages_articles')

extracted_external_links_dir = join(extracted_dir, 'external_links')
extracted_external_links_filename = 'all'

extracted_template_names_dir = dict()
extracted_template_names_dir['fa'] = join(extracted_dir, 'fa_template_names')
extracted_template_names_dir['en'] = join(extracted_dir, 'en_template_names')

extracted_lang_links_dir = join(extracted_dir, 'lang_links')
extracted_en_lang_link_filename = 'en'
extracted_ar_lang_link_filename = 'ar'

extracted_page_ids_dir = join(extracted_dir, 'page_ids')
extracted_page_ids_filename = 'all'

extracted_pages_dir = dict()
extracted_pages_dir['fa'] = join(extracted_dir, 'fa_pages')
extracted_pages_dir['en'] = join(extracted_dir, 'en_pages')

extracted_pages_with_infobox_dir = dict()
extracted_pages_with_infobox_dir['fa'] = join(extracted_pages_dir['fa'], 'with_infobox')
extracted_pages_with_infobox_dir['en'] = join(extracted_pages_dir['en'], 'with_infobox')

extracted_pages_without_infobox_dir = dict()
extracted_pages_without_infobox_dir['fa'] = join(extracted_pages_dir['fa'], 'without_infobox')
extracted_pages_without_infobox_dir['en'] = join(extracted_pages_dir['en'], 'without_infobox')


extracted_redirects_dir = join(extracted_dir, 'redirects')
extracted_reverse_redirects_dir = join(extracted_dir, 'reverse_redirects')

extracted_revision_ids_dir = join(extracted_dir, 'revision_ids')
extracted_wiki_links_dir = join(extracted_dir, 'wiki_links')
extracted_wiki_texts_dir = join(extracted_dir, 'wiki_texts')

extracted_with_infobox_dir = join(extracted_dir, 'with_infobox')
extracted_without_infobox_dir = join(extracted_dir, 'without_infobox')
extracted_path_name_dir = join(extracted_dir, 'path')
extracted_image_name_dir = join(extracted_dir, 'image')


reorganized_infoboxes_dir = join(refined_dir, 'infoboxes')
final_tuples_dir = join(refined_dir, 'tuples')
infobox_counters_dir = join(refined_dir, 'infobox_counters')
infobox_predicates_dir = join(refined_dir, 'infobox_predicates')
infobox_mapping_dir = join(refined_dir, 'infobox_mapping')
infobox_mapping_filename = 'mappings'


infobox_flags_en = sorted(['reactionbox', 'ionbox', 'infobox', 'taxobox', 'drugbox', 'geobox', 'planetbox', 'chembox',
                           'starbox', 'drugclassbox', 'speciesbox', 'comiccharacterbox'], key=len, reverse=True)

infobox_flags_fa = sorted(['جعبه اطلاعات', 'جعبه'], key=len, reverse=True)

redirect_flags = ['#تغییر_مسیر', '#تغییرمسیر', '#REDIRECT', '#redirect']

disambigution_flags = ['رفع‌ابهام‌', 'رفع ابهام', 'ابهام‌زدایی', 'ابهام زدایی']

stub_flag_en = ['stub']

stub_flag_fa = ['خرد']

see_also_in_fa = 'جستارهای وابسته'


extract_bz2_dump_information_parameters = {
    'extract_abstracts': False,
    'extract_page_ids': False,
    'extract_revision_ids': False,
    'extract_wiki_texts': False,
    'extract_pages': False,
    'extract_infoboxes': False,
    'extract_disambiguations': False,
    'extract_template_names': False,
    'lang': None
}


class ConnectionString:
    host = '194.225.227.161'
    port = 3306
    user = ''
    password = ''
    db = 'knowledge_graph'
    use_unicode = True
    charset = "utf8"


wiki_templates_transcluded_on_pages_table_name = 'wiki_templates_transcluded_on_pages'
wiki_templates_transcluded_on_pages_table_structure = {
    'template_name': 'varchar(256) default NULL',
    'template_type': 'varchar(256) NOT NULL',
    'language': 'varchar(10) NOT NULL',
    'count': 'int(11) NOT NULL'
}
wiki_templates_transcluded_on_pages_key_order = ['template_name', 'template_type', 'language', 'count']


wiki_template_mapping_table_name = 'wiki_template_mapping'
wiki_template_mapping_table_structure = {
    'id': 'int NOT NULL AUTO_INCREMENT',
    'template_name_fa': 'varchar(500)',
    'template_name_en': 'varchar(500)',
    'approved': 'tinyint default NULL',
    'extraction_from': 'varchar(100)',
}
wiki_template_mapping_key_order = ['id', 'template_name_fa', 'template_name_en', 'approved', 'extraction_from']
wiki_template_mapping_primary_keys = ['id']
wiki_template_mapping_unique_keys = {'template_name_en_fa': ['template_name_fa', 'template_name_en']}


disambiguation_regex = re.compile(r'(?:^\* *\[\[.+?\]\])')
