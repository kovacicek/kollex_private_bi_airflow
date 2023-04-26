import logging

import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
from scipy.sparse import csr_matrix
import sparse_dot_topn.sparse_dot_topn as ct
from include.db import prepare_pg_connection


logging.getLogger().setLevel(logging.INFO)


def data_preparation():
    engine = prepare_pg_connection()
    all_skus_sql = """
        select
            identifier as sku,
            base_code,
            amount_single_unit,
            net_content,
            enablement,
            CONCAT(brand, ' ', title, ' ', amount_single_unit, 'x', net_content) as "sku_title"
        from prod_raw_layer.all_skus
        where enablement > 0 and type_single_unit <> 'FREAK (abweichendes Design, Rücksprache halten)' 
        and net_content is not null
        and base_code is not null
    """

    df_all_skus = pd.read_sql_query(all_skus_sql, engine)
    # only for testing usually it will be spreadsheet
    df = pd.read_csv("test_input.csv")
    return df, df_all_skus


def ngrams(string, n=3):
    string = re.sub(r'[,-./]|\sBD',r'', string)
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]


def cossim_top(A, B, ntop, lower_bound=0):
    """
    names don't follow PEP8, because follow formula above.
    """
    A = A.tocsr()
    B = B.tocsr()
    m, _ = A.shape
    _, n = B.shape

    idx_dtype = np.int32

    nnz_max = m * ntop

    index_pointer = np.zeros(m + 1, dtype=idx_dtype)
    indices = np.zeros(nnz_max, dtype=idx_dtype)
    data = np.zeros(nnz_max, dtype=A.dtype)

    ct.sparse_dot_topn(
        m, n, np.asarray(A.indptr, dtype=idx_dtype),
        np.asarray(A.indices, dtype=idx_dtype),
        A.data,
        np.asarray(B.indptr, dtype=idx_dtype),
        np.asarray(B.indices, dtype=idx_dtype),
        B.data,
        ntop,
        lower_bound,
        index_pointer, indices, data)
    return csr_matrix((data, indices, index_pointer), shape=(m, n))


def get_matches_df(sparse_matrix, A, B, top=100):
    non_zeros = sparse_matrix.nonzero()
    sparse_rows = non_zeros[0]
    sparse_cols = non_zeros[1]

    if top:
        nr_matches = top
    else:
        nr_matches = sparse_cols.size

    left_side = np.empty([nr_matches], dtype=object)
    right_side = np.empty([nr_matches], dtype=object)
    similarity = np.zeros(nr_matches)

    for index in range(0, nr_matches):
        left_side[index] = A[sparse_rows[index]]
        right_side[index] = B[sparse_cols[index]]
        similarity[index] = sparse_matrix.data[index]

    return pd.DataFrame({'left_side': left_side,
                         'right_side': right_side,
                         'similarity': similarity})


def run():
    logging.info("Data preparation")
    df, df_all_skus = data_preparation()
    logging.info("Name vectorization")
    vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
    tf_idf_matrix_all_skus = vectorizer.fit_transform(df_all_skus['sku_title'])
    tf_idf_matrix_input = vectorizer.transform(df['name'])

    matches = cossim_top(tf_idf_matrix_input,
                         tf_idf_matrix_all_skus.transpose(), 1, 0)
    matches_df = get_matches_df(matches, df['name'], df_all_skus['sku_title'],
                                top=0)
    matches_df = matches_df[matches_df['similarity'] > 0.5]
    matches_df.rename(
        columns={'left_side': 'name_input', 'right_side': 'name_all_skus'},
        inplace=True)

    df_final = pd.merge(
        matches_df,
        df_all_skus,
        how='left',
        left_on='name_all_skus',
        right_on='sku_title'
    )

    df_final.rename(columns={'base_code': 'base_code_all_skus',
                             'net_content': 'base_unit_content_all_skus',
                             'amount_single_unit': 'no_of_base_units_all_skus'},
                    inplace=True)
    df_final = pd.merge(
        df_final,
        df,
        how='left',
        left_on='name_input',
        right_on='name'
    )

    df_final.rename(columns={'no_of_base_units': 'no_of_base_units_input',
                             'base_unit_content': 'base_unit_content_input'},
                    inplace=True)

    df_final = df_final[[
        'sku',
        'gfgh_id',
        'name_input',
        'no_of_base_units_input',
        'base_unit_content_input',
        'name_all_skus',
        'no_of_base_units_all_skus',
        'base_unit_content_all_skus',
        'similarity'
    ]].copy()
    logging.info("Sharing results")

    df_final['no_of_base_units_input'] = df_final[
        'no_of_base_units_input'].astype(int)
    df_final['no_of_base_units_all_skus'] = df_final[
        'no_of_base_units_all_skus'].astype(int)
    df_final['base_unit_content_all_skus'] = df_final[
        'base_unit_content_all_skus'].apply(lambda row: row.replace(',', '.'))
    df_final['base_unit_content_all_skus'] = df_final[
        'base_unit_content_all_skus'].astype(float)

    name_matching_final = df_final[df_final['no_of_base_units_all_skus'] == df_final[
        'no_of_base_units_input']]
    name_matching_final = name_matching_final.drop_duplicates(subset='gfgh_id')
    name_matching_final.to_sql(
        schema="prod_raw_layer",
        name="result_name_matching",
        con=prepare_pg_connection(),
        if_exists="drop",
        method="multi"
    )


if __name__ == "__main__":
    run()
