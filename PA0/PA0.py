import json
import ctypes
from dask.distributed import Client
import dask.dataframe as dd
import dask.array as da
import dask.bag as db

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)
    
def PA0(path_to_user_reviews_csv):
    client = Client()
    # Helps fix any memory leaks.
    client.run(trim_memory)
    client = client.restart()

    # Read the csv
    ddf = dd.read_csv(path_to_user_reviews_csv)
    # Drop the unneccessary columns
    ddf = ddf.drop(columns=['reviewerName', 'reviewText', 'summary'], errors='ignore')

    # Extract year
    ddf['year'] = ddf['unixReviewTime'].map(lambda x: (x // 31536000) + 1970, meta=('unixReviewTime', 'float64'))
    # Extract data from the helpful column
    ddf['helpful_votes'] = ddf['helpful'].map(lambda x: eval(x)[0] if isinstance(x, str) else x[0], meta=('helpful_votes', int))
    ddf['total_votes'] = ddf['helpful'].map(lambda x: eval(x)[1] if isinstance(x, str) else x[1], meta=('total_votes', int))

    # Drop the unnecessary columns
    ddf = ddf.drop(columns=['unixreviewTime', 'reviewTime', 'helpful'], errors='ignore')
    
    result = ddf.groupby('reviewerID').agg({
        # Number_products_rated: reviewerID + asin -> groupby + total
        'asin': 'count',
        # Avg_ratings: reviewerID + overall -> groupby + mean()
        'overall': 'mean',
        # Reviewing_since: reviewerID + reviewTime -> groupby + min()
        'year': 'min',
        # Helpful_votes: reviewerID + helpful -> groupby + sum()
        'helpful_votes': 'sum',
        # Total_votes: reviewerID + helpful -> groupby + sum()
        'total_votes': 'sum'
    })

    # Rename the columns
    result = result.rename(columns={
        'asin': 'number_products_rated',
        'overall': 'avg_ratings',
        'year': 'reviewing_since'
    }).reset_index()

    # Output    
    submit = result.describe().compute().round(2)    
    with open('results_PA0.json', 'w') as outfile: 
        json.dump(json.loads(submit.to_json()), outfile)

# Running the code
if __name__ == '__main__':
    PA0('user_reviews.csv')
