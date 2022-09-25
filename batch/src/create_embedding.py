import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer


def main():

    wiki_df = pd.read_csv('../data/wiki_abstract.csv', nrows=1000)

    model = SentenceTransformer(
        'cl-tohoku/bert-base-japanese-whole-word-masking')

    embeddings = model.encode(wiki_df['abstract'].tolist(),
                              normalize_embeddings=True,
                              show_progress_bar=True)

    np.save('../data/embeddings', embeddings)


if __name__ == '__main__':
    main()
