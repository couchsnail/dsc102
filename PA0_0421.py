import json
import ctypes
import dask 
from dask.distributed import Client
import numpy as np
import pandas as pd
import dask.dataframe as dd
import dask.array as da
import dask.bag as db
import datetime

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)

# def process(csv_name):
    # # Load Dask DataFrame
    # ddf = dd.read_csv(csv_name)
    # # We don't need these columns, so drop them
    # ddf = ddf.drop(columns=['reviewerName', 'reviewText', 'summary'], errors='ignore')

    # # number_products_rated: reviewerID + asin -> groupby + total
    # number_products_rated = ddf.groupby('reviewerID').asin.count().rename('number_products_rated')

    # # avg_ratings: reviewerID + overall -> groupby + mean()
    # avg_ratings = ddf.groupby('reviewerID').overall.mean().rename('avg_ratings')

    # # reviewing_since: reviewerID + reviewTime -> groupby + min() -> get last 4 characters of string
    # # have a preprocessing step where we get the year from the reviewTime string
    # ddf['year'] = ddf['reviewTime'].map(lambda x: int(x[-4:]), meta=('reviewTime', int))
    # reviewing_since = ddf.groupby('reviewerID').year.min().rename('reviewing_since')

    # # Note: 'helpful' despite looking like a tuple is a string???
    # # helpful_votes: reviewerID + helpful -> groupby + sum() -> get the first entry in the tuple
    # ddf['helpful_votes'] = ddf['helpful'].map(lambda x: eval(x)[0] if isinstance(x, str) else x[0], meta=('helpful_votes', int))
    # helpful_votes = ddf.groupby('reviewerID').helpful_votes.sum().rename('helpful_votes')
    
    # # total_votes: reviewerID + helpful -> groupby + sum() -> get the second entry in the tuple
    # ddf['total_votes'] = ddf['helpful'].map(lambda x: eval(x)[1] if isinstance(x, str) else x[1], meta=('total_votes', int))
    # total_votes = ddf.groupby('reviewerID').total_votes.sum().rename('total_votes')

    # # Creating the final dataframe
    # result = dd.concat([number_products_rated, avg_ratings, reviewing_since, helpful_votes, total_votes], axis=1).reset_index()

    # return result

def process(csv_name):
    ddf = dd.read_csv(csv_name)
    ddf = ddf.drop(columns=['reviewerName', 'reviewText', 'summary'], errors='ignore')

    # Extract year
    ddf['year'] = dd.to_datetime(ddf['reviewTime'], errors='coerce').dt.year

    # Parse helpful field
    ddf['helpful_votes'] = ddf['helpful'].map(lambda x: eval(x)[0] if isinstance(x, str) else x[0], meta=('helpful_votes', int))
    ddf['total_votes'] = ddf['helpful'].map(lambda x: eval(x)[1] if isinstance(x, str) else x[1], meta=('total_votes', int))

    # One groupby with all aggregations
    result = ddf.groupby('reviewerID').agg({
        'asin': 'count',
        'overall': 'mean',
        'year': 'min',
        'helpful_votes': 'sum',
        'total_votes': 'sum'
    }).rename(columns={
        'asin': 'number_products_rated',
        'overall': 'avg_ratings',
        'year': 'reviewing_since'
    }).reset_index()

    return result

def PA0(path_to_user_reviews_csv):
    client = Client()
    # Helps fix any memory leaks.
    client.run(trim_memory)
    client = client.restart()

    result = process(path_to_user_reviews_csv)
    
    submit = result.describe().compute().round(2)    
    with open('results_PA0.json', 'w') as outfile: 
        json.dump(json.loads(submit.to_json()), outfile)

# Running the code
if __name__ == '__main__':
    PA0('user_reviews.csv')

##################################################################################
# This part below was for testing purposes
# drop reviewername, reviewertext, summary
def create_subset(csv_name):
    ddf = dd.read_csv(csv_name)
    ddf = ddf.repartition(npartitions = 5000)
    ddf.get_partition(0).to_csv('subset.csv', single_file = True)

# create_subset('user_reviews.csv')
# def preprocess(csv_name):
#     ddf = dd.read_csv(csv_name)
    
#     prod_rated = ddf[['reviewerID', 'asin']].groupby(by='reviewerID').count()
#     number_products_rated = prod_rated.compute()
#     reviewerID = number_products_rated.index

#     avg_rating = ddf[['reviewerID', 'overall']].groupby(by='reviewerID').mean()['overall']
#     avg_ratings = avg_rating.compute()
    
#     # ddf['year'] = ddf['reviewTime'].apply(lambda x: int(x[-4:]))
#     # reviewing_since = ddf[['reviewerID', 'year']].groupby(by='revewerID').min()['year']
#     # ddf['helpful_votes'] = ddf['helpful'].apply(lambda x: x[0])
#     # ddf['total_votes'] = ddf['helpful'].apply(lambda x: x[1])
#     # helpful_votes = ddf[['reviewerID', 'helpful_votes']].groupby(by='reviewerID').sum()['helpful_votes']
#     # total_votes = ddf[['reviewerID', 'total_votes']].groupby(by='reviewerID').sum()['total_votes']
    
#     result = pd.DataFrame({number_products_rated, avg_ratings})
#     # result = pd.DataFrame({'reviewerID': ddf['reivewerID'], 'number_products_rated': prod_rated, 'avg_ratings': avg_rating, 'reviewing_since': reviewing_since, 'helpful_votes': helpful_votes, 'total_votes': total_votes})

#     dask_result = dd.from_pandas(number_products_rated)
#     dask_result.get_partition(0).to_csv('test.csv', single_file = True)

#     return dask_result