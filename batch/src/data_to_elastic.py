from glob import glob

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import numpy as np
from tqdm import tqdm


class ElasticLoader:

    def __init__(self, url, auth):

        self.es = Elasticsearch(hosts=url,
                                basic_auth=auth,
                                verify_certs=False,
                                timeout=120,
                                max_retries=5)

    def create_index(self, index):

        index_config = {
            'settings': {
                'number_of_shards': 3,
                'index': {
                    'similarity': {
                        'default': {
                            'type': 'BM25'
                        }
                    },
                    'analysis': {
                        'kuromoji_tokenizer': {
                            'kuromoji': {
                                'type': 'kuromoji_tokenizer'
                            }
                        },
                        'analyzer': {
                            'kuromoji_analyzer': {
                                'tokenizer': 'kuromoji_tokenizer',
                                'type': 'custom'
                            }
                        }
                    }
                }
            },
            'mapping': {
                'properties': {
                    'title': {
                        'type': 'text'
                    },
                    'abstract': {'type', 'text'},
                    'vector': {
                        'type': 'dense_vector',
                        'dims': 768,
                        'index': 'true',
                        'similarity': 'dot_product'
                    }
                }
            }
        }

        self.index = index
        self.index_config = index_config

    def load_to_elastic(self, title_list, abstract_list, vector_list):

        docs = [{
            'title': title,
            'abstract': abstract,
            'vector': vector,
            '_index': 'wiki_index'
        } for title, abstract, vector in zip(title_list,
                                             abstract_list,
                                             vector_list)]
        bulk(self.es, docs)


def main():

    print('init es')
    url = 'https://localhost:9200'
    auth = ('elastic', 'elastic')
    el = ElasticLoader(url, auth)

    print('create_index')
    # el.create_index('sample_wiki')

    print('load data')
    doc_list = [doc for doc in glob('../data/wiki_abstract_*')]
    doc_list.sort()
    embed_list = [embed for embed in glob('../data/embeddings_*.csv.npy')]
    embed_list.sort()

    print('load to es')
    for doc, embed in tqdm(zip(doc_list, embed_list)):
        wiki_df = pd.read_csv(doc)
        title_list = wiki_df['title'].tolist()
        abstract_list = wiki_df['abstract'].tolist()
        vector_list = list(np.load(embed))
        try:
            el.load_to_elastic(title_list, abstract_list, vector_list)
        except Exception as e:
            print(e)

    el.es.close()


if __name__ == '__main__':
    main()
