from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
import numpy as np


class ElasticLoader:

    def __init__(self, url, auth):

        self.es = Elasticsearch(hosts=url,
                                basic_auth=auth,
                                verify_certs=False)

    def create_index(self, index):

        index_config = {
            'settings': {
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
                'dynamic': 'true',
                '_source': {
                    'enabled': 'true'
                },
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
            '_index': self.index
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
    el.create_index('sample_index')

    print('load to es')
    wiki_df = pd.read_csv('../data/wiki_abstract.csv', nrows=1000)
    title_list = wiki_df['title'].tolist()
    abstract_list = wiki_df['abstract'].tolist()
    vector_list = list(np.load('../data/embeddings.npy'))
    el.load_to_elastic(title_list, abstract_list, vector_list)


if __name__ == '__main__':
    main()
