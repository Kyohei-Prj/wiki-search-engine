from glob import glob
from concurrent.futures import ProcessPoolExecutor

import pandas as pd


def main():

    doc_list = [doc for doc in glob('../data/text/*/wiki_*')]
    with ProcessPoolExecutor(max_workers=4) as exe:
        result = exe.map(extract_title_abstract, doc_list)

    wiki_df = pd.concat(result)
    wiki_df.to_csv('../data/wiki_abstract.csv', index=False)


def extract_title_abstract(path):

    with open(path, mode='r') as fn:
        text = fn.readlines()

    target = ' \n'
    while target in text:
        text.remove(target)

    title_list = []
    abstract_list = []
    for i, tx in enumerate(text):
        if 'title=' in tx:
            title = text[i+1].replace('\n', '')
            title_list.append(title)
            abstract = text[i+2].replace('\n', '') + ' ' + text[i+3].replace('\n', '')
            abstract_list.append(abstract)

    df = pd.DataFrame()
    df['title'] = title_list
    df['abstract'] = abstract_list

    return df


if __name__ == '__main__':
    main()
