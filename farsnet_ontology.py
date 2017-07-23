import csv

import Config
import DataUtils
import hazm

def map_farsnet_kg_ontology(input_filename):
    input_ontology_filename = DataUtils.join(Config.farsnet_ontology,
                                                       Config.farsnet_ontology_filename)
    output_farsnet_map_ontology_filename = DataUtils.join(Config.farsnet_ontology, Config.farsnet_map_ontology_filename)

    normalizer = hazm.Normalizer()
    print('input file ' + input_filename)

    with open(input_ontology_filename, 'r') as input_file_ontology, \
            open(output_farsnet_map_ontology_filename, 'a') as output_file:
        csv_reader_ontology, csv_writer = csv.reader(input_file_ontology), csv.writer(output_file)
        for line_ontology in csv_reader_ontology:
            with open(input_filename, 'r') as input_file_graph:
                csv_reader_graph = csv.reader(input_file_graph)
                for line_graph in csv_reader_graph:
                    item = normalizer.normalize(line_graph[1])
                    if normalizer.normalize(line_ontology[0]) == item:
                        print(item)
                        csv_writer.writerow([line_graph[0], item, line_graph[3]])


def not_map_farsnet_kg_ontology():
    input_ontology_filename = DataUtils.join(Config.farsnet_ontology,
                                                       Config.farsnet_ontology_filename)
    input_farsnet_map_ontology_filename = DataUtils.join(Config.farsnet_ontology, Config.farsnet_map_ontology_filename)
    output_farsnet_not_map_ontology_filename = DataUtils.join(Config.farsnet_ontology, Config.farsnet_not_map_ontology_filename)

    normalizer = hazm.Normalizer()
    flag_find = False
    item = 'word'
    with open(input_ontology_filename, 'r') as input_file_ontology, \
            open(output_farsnet_not_map_ontology_filename, 'a') as output_file:
        csv_reader_ontology, csv_writer = csv.reader(input_file_ontology), csv.writer(output_file)
        for line_ontology in csv_reader_ontology:
            if not flag_find:
                csv_writer.writerow([item])
                print(item)
            item = normalizer.normalize(line_ontology[0])
            flag_find = False
            with open(input_farsnet_map_ontology_filename, 'r') as input_file_map:
                csv_reader_graph = csv.reader(input_file_map)

                for line_map in csv_reader_graph:
                    if item == normalizer.normalize(line_map[1]):
                        flag_find = True
                        break;





