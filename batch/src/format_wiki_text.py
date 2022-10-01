from glob import glob
from concurrent.futures import ProcessPoolExecutor

import pandas as pd


def main():

    doc_list = [doc for doc in glob('../data/text/*/wiki_*')]

    doc_size = len(doc_list)
    split_size = 8
    chunk_size = int(doc_size / split_size)

    for i in range(split_size):
        if i == (split_size-1):
            run_extractor(doc_list[i*chunk_size:], i)
        else:
            run_extractor(doc_list[i*chunk_size: chunk_size*(i+1)], i)


def run_extractor(doc_list, chunk):

    with ProcessPoolExecutor(max_workers=15) as exe:
        result = exe.map(extract_title_abstract, doc_list)

    wiki_df = pd.concat(result)
    save_path = '../data/wiki_abstract_' + str(chunk) + '.csv'
    wiki_df.to_csv(save_path, index=False)


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
