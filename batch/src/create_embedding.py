from glob import glob

import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('cl-tohoku/bert-base-japanese-whole-word-masking')


def main():

    doc_list = [doc for doc in glob('../data/wiki_abstract_*.csv')]

    for i, doc in enumerate(doc_list):
        print(f'{i+1}/{len(doc_list)}')
        create_embedding(doc, i)


def create_embedding(file_path, chunk):

    wiki_df = pd.read_csv(file_path)
    text_list = [
        str(title) + ' ' + str(abstract) for title, abstract in zip(
            wiki_df['title'].tolist(), wiki_df['abstract'].tolist())
    ]

    embeddings = model.encode(text_list,
                              normalize_embeddings=True,
                              show_progress_bar=True,
                              batch_size=32)

    save_path = '../data/embeddings_' + str(chunk)
    np.save(save_path, embeddings)


if __name__ == '__main__':
    main()
