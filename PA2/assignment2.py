import os
import pyspark.sql.functions as F
import pyspark.sql.types as T
from utilities import SEED
# import any other dependencies you want, but make sure only to use the ones
# availiable on AWS EMR

# ---------------- choose input format, dataframe or rdd ----------------------
INPUT_FORMAT = 'dataframe'  # change to 'rdd' if you wish to use rdd inputs
# -----------------------------------------------------------------------------
if INPUT_FORMAT == 'dataframe':
    import pyspark.ml as M
    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
if INPUT_FORMAT == 'koalas':
    import databricks.koalas as ks
elif INPUT_FORMAT == 'rdd':
    import pyspark.mllib as M
    from pyspark.mllib.feature import Word2Vec
    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.linalg.distributed import RowMatrix
    from pyspark.mllib.tree import DecisionTree
    from pyspark.mllib.regression import LabeledPoint
    from pyspark.mllib.linalg import DenseVector
    from pyspark.mllib.evaluation import RegressionMetrics


import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, explode, mean, variance, size, count, when
from pyspark.sql import functions as F
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


# ---------- Begin definition of helper functions, if you need any ------------

# def task_1_helper():
#   pass

# -----------------------------------------------------------------------------


# %load -s task_1 assignment2.py
def task_1(data_io, review_data, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    overall_column = 'overall'
    # Outputs:
    mean_rating_column = 'meanRating'
    count_rating_column = 'countRating'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    joined = (review_data.select(['asin', 'overall', 'reviewerID'])
        .groupBy('asin')
        .agg({'reviewerID' : 'count', 'overall' : 'mean'}))
    joined_stats = product_data.select(['asin']).join(joined, 'asin', 'left')
    
    # Produces columns 'avg(overall)' and 'count(title)'

    #joined_stats = joined_stats.withColumnsRenamed({: 'mean_rating', 'count(title)':'count_rating'})
    mean_meanRating = joined_stats.select(F.avg(F.col('avg(overall)'))).head()[0]
    #print(mean_meanRating)
    count_total = joined_stats.count()
    variance_meanRating = joined_stats.select(F.variance(F.col('avg(overall)'))).head()[0]
    numNulls_meanRating = count_total - joined_stats.na.drop().count()
    mean_countRating = joined_stats.select(F.avg(F.col('count(reviewerID)'))).head()[0]
    variance_countRating = joined_stats.select(F.variance(F.col('count(reviewerID)'))).head()[0]
    numNulls_countRating = count_total - joined_stats.na.drop().count()
    
    #count_total = len(joined_stats)
    



    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    # Calculate the values programmaticly. Do not change the keys and do not
    # hard-code values in the dict. Your submission will be evaluated with
    # different inputs.
    # Modify the values of the following dictionary accordingly.
    res = {
        'count_total': None,
        'mean_meanRating': None,
        'variance_meanRating': None,
        'numNulls_meanRating': None,
        'mean_countRating': None,
        'variance_countRating': None,
        'numNulls_countRating': None
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_meanRating'] = mean_meanRating
    res['variance_meanRating'] = variance_meanRating
    res['numNulls_meanRating'] = numNulls_meanRating
    res['mean_countRating'] = mean_countRating
    res['variance_countRating'] = variance_countRating
    res['numNulls_countRating'] = numNulls_countRating
    



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_1')
    return res
    # -------------------------------------------------------------------------


# %load -s task_2 assignment2.py
def task_2(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    salesRank_column = 'salesRank'
    categories_column = 'categories'
    asin_column = 'asin'
    # Outputs:
    category_column = 'category'
    bestSalesCategory_column = 'bestSalesCategory'
    bestSalesRank_column = 'bestSalesRank'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------

    categories = product_data.select(
        F.col("categories"),
        F.when(
            (F.col("categories").isNotNull()) &
            (F.size(F.col("categories")) > 0) &
            (F.size(F.col("categories")[0]) > 0) &
            (F.trim(F.col("categories")[0][0]) != ''),
            F.col("categories")[0][0]
        ).otherwise(None)
    )
        
    salesRank_data = product_data.select(['asin', 'salesRank', F.explode(F.col('salesRank'))])
    salesRank_data = salesRank_data.withColumnRenamed('key', 'bestSalesCategory') \
                               .withColumnRenamed('value','bestSalesRank')
    salesRank_data = salesRank_data.drop('salesRank')
    count_total = categories.count()
    mean_bestSalesRank = salesRank_data.select(F.avg(F.col('bestSalesRank'))).head()[0]
    variance_bestSalesRank = salesRank_data.select(F.variance(F.col('bestSalesRank'))).head()[0]
    numNulls_category = count_total - categories.dropna(subset = categories.columns[1]).count()
    countDistinct_category = categories.dropna().select(F.count_distinct(F.col(categories.columns[1]))).head()[0]
    numNulls_bestSalesCategory = count_total - salesRank_data.dropna(subset = 'bestSalesCategory').count() #0 nulls.select('bestSalesRank')[0]
    countDistinct_bestSalesCategory = salesRank_data.select((F.count_distinct(F.col('bestSalesCategory')))).head()[0]
    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': count_total,
        'mean_bestSalesRank':mean_bestSalesRank,
        'variance_bestSalesRank': variance_bestSalesRank,
        'numNulls_category': numNulls_category,
        'countDistinct_category':countDistinct_category,
        'numNulls_bestSalesCategory': numNulls_bestSalesCategory ,
        'countDistinct_bestSalesCategory': countDistinct_bestSalesCategory
    }
    # Modify res:
    res['count_total'] = count_total
    res['mean_bestSalesRank'] = mean_bestSalesRank
    res['variance_bestSalesRank'] = variance_bestSalesRank
    res['numNulls_category'] = numNulls_category
    res['countDistinct_category'] = countDistinct_category
    res['numNulls_bestSalesCategory']= numNulls_bestSalesCategory
    res['countDistinct_bestSalesCategory'] = countDistinct_bestSalesCategory
     

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_2')
    return res#, salesRank_data
    # -------------------------------------------------------------------------


def task_3(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    asin_column = 'asin'
    price_column = 'price'
    attribute = 'also_viewed'
    related_column = 'related'
    # Outputs:
    meanPriceAlsoViewed_column = 'meanPriceAlsoViewed'
    countAlsoViewed_column = 'countAlsoViewed'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    
    price_df = product_data.select(asin_column, price_column).where(col(price_column).isNotNull())

    
    also_viewed_df = product_data.select(
        col(asin_column),
        col(f"{related_column}.{attribute}").alias("also_viewed")
    )

    
    count_df = also_viewed_df.withColumn(
        countAlsoViewed_column,
        when(col("also_viewed").isNull() | (size("also_viewed") == 0), None)
        .otherwise(size("also_viewed"))
    ).select(asin_column, countAlsoViewed_column)

    # wanting to lookup prices
    
    exp = also_viewed_df.where(col("also_viewed").isNotNull()) \
        .withColumn("viewed_asin", explode("also_viewed"))

   # wanting valid prices
    joined = exp.join(
        price_df,
        exp["viewed_asin"] == price_df[asin_column],
        how="left"
    ).select(exp[asin_column].alias("main_asin"), "price")

   
    filtered = joined.where(col("price").isNotNull())

    
    mean_price_df = filtered.groupBy("main_asin").agg(
        mean("price").alias(meanPriceAlsoViewed_column)
    )


    result_df = product_data.select(asin_column) \
        .join(count_df, on=asin_column, how="left") \
        .join(mean_price_df, product_data[asin_column] == mean_price_df["main_asin"], how="left") \
        .drop("main_asin")
   #getting the stats that they want

    summary = result_df.selectExpr(
        "COUNT(*) as count_total",
        f"AVG({meanPriceAlsoViewed_column}) as mean_meanPriceAlsoViewed",
        f"VARIANCE({meanPriceAlsoViewed_column}) as variance_meanPriceAlsoViewed",
        f"SUM(CASE WHEN {meanPriceAlsoViewed_column} IS NULL THEN 1 ELSE 0 END) as numNulls_meanPriceAlsoViewed",
        f"AVG({countAlsoViewed_column}) as mean_countAlsoViewed",
        f"VARIANCE({countAlsoViewed_column}) as variance_countAlsoViewed",
        f"SUM(CASE WHEN {countAlsoViewed_column} IS NULL THEN 1 ELSE 0 END) as numNulls_countAlsoViewed"
    ).first().asDict()
    

  
    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': summary['count_total'],
        'mean_meanPriceAlsoViewed': summary['mean_meanPriceAlsoViewed'],
        'variance_meanPriceAlsoViewed': summary['variance_meanPriceAlsoViewed'],
        'numNulls_meanPriceAlsoViewed': summary['numNulls_meanPriceAlsoViewed'],
        'mean_countAlsoViewed': summary['mean_countAlsoViewed'],
        'variance_countAlsoViewed': summary['variance_countAlsoViewed'],
        'numNulls_countAlsoViewed': summary['numNulls_countAlsoViewed']
    }

 
    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_3')
    return res
    # -------------------------------------------------------------------------


def task_4(data_io, product_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    price_column = 'price'
    title_column = 'title'
    # Outputs:
    meanImputedPrice_column = 'meanImputedPrice'
    medianImputedPrice_column = 'medianImputedPrice'
    unknownImputedTitle_column = 'unknownImputedTitle'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    price_data = product_data.select(['price', 'title'])
    price_data = price_data.withColumn('price', F.col('price').cast(FloatType()))
    
    mean_price = price_data.select(F.avg('price')).collect()[0][0]
    price_data = price_data.withColumn(
        'meanImputedPrice',
        F.when(F.col('price').isNotNull(), F.col('price')).otherwise(F.lit(mean_price))
    )
    
    median_price = price_data.approxQuantile("price", [0.5], 0.01)[0]
    price_data = price_data.withColumn(
        'medianImputedPrice',
        F.when(F.col('price').isNotNull(), F.col('price')).otherwise(F.lit(median_price))
    )
    
    price_data = price_data.withColumn(
        'unknownImputedTitle',
        F.when(F.col('title').isNotNull() & (F.col('title')!=''), F.col('title')).otherwise(F.lit('unknown'))
    )
    
    # Calculating values for res
    count_total = price_data.count()
    mean_meanImputedPrice = price_data.select(F.avg('meanImputedPrice')).collect()[0][0]
    variance_meanImputedPrice = price_data.select(F.variance('meanImputedPrice')).collect()[0][0]
    numNulls_meanImputedPrice = count_total - price_data.select('meanImputedPrice').na.drop().count()
    mean_medianImputedPrice = price_data.select(F.avg('medianImputedPrice')).collect()[0][0]
    variance_medianImputedPrice = price_data.select(F.variance('medianImputedPrice')).collect()[0][0]
    numNulls_medianImputedPrice = count_total - price_data.select('medianImputedPrice').na.drop().count()
    numUnknowns_unknownImputedTitle = price_data.filter(F.col('unknownImputedTitle') == 'unknown').count()




    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'mean_meanImputedPrice': None,
        'variance_meanImputedPrice': None,
        'numNulls_meanImputedPrice': None,
        'mean_medianImputedPrice': None,
        'variance_medianImputedPrice': None,
        'numNulls_medianImputedPrice': None,
        'numUnknowns_unknownImputedTitle': None
    }
    
    
    # Modify res:
    res['count_total'] = count_total
    res['mean_meanImputedPrice' = mean_meanImputedPrice
    res['variance_meanImputedPrice'] = variance_meanImputedPrice
    res['numNulls_meanImputedPrice'] = numNulls_meanImputedPrice
    res['mean_medianImputedPrice'] = mean_medianImputedPrice
    res['variance_medianImputedPrice'] = variance_medianImputedPrice
    res['numNulls_medianImputedPrice'] = numNulls_medianImputedPrice
    res['numUnknowns_unknownImputedTitle'] = numUnknowns_unknownImputedTitle



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_4')
    return res
    # -------------------------------------------------------------------------



def task_5(data_io, product_processed_data, word_0, word_1, word_2):
    # -----------------------------Column names--------------------------------
    # Inputs:
    title_column = 'title'
    # Outputs:
    titleArray_column = 'titleArray'
    titleVector_column = 'titleVector'
    # -------------------------------------------------------------------------

    # ---------------------- Your implementation begins------------------------
    title_data = product_processed_data.select('title')
    product_processed_data_output = title_data.withColumn('titleArray', F.split(F.lower(F.col('title')), " "))
    word2Vec = M.feature.Word2Vec(
        vectorSize=16, 
        minCount=100, 
        seed=SEED, 
        inputCol="titleArray", 
        outputCol="titleVector", 
        numPartitions=4
    )
    model = word2Vec.fit(product_processed_data_output)
    product_processed_data_output = model.transform(product_processed_data_output)

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'size_vocabulary': None,
        'word_0_synonyms': [(None, None), ],
        'word_1_synonyms': [(None, None), ],
        'word_2_synonyms': [(None, None), ]
    }
    # Modify res:
    res['count_total'] = product_processed_data_output.count()
    res['size_vocabulary'] = model.getVectors().count()
    for name, word in zip(
        ['word_0_synonyms', 'word_1_synonyms', 'word_2_synonyms'],
        [word_0, word_1, word_2]
    ):
        res[name] = model.findSynonymsArray(word, 10)
    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_5')
    return res
    # -------------------------------------------------------------------------


def task_6(data_io, product_processed_data):
    # -----------------------------Column names--------------------------------
    # Inputs:
    category_column = 'category'
    # Outputs:
    categoryIndex_column = 'categoryIndex'
    categoryOneHot_column = 'categoryOneHot'
    categoryPCA_column = 'categoryPCA'
    # -------------------------------------------------------------------------    

    # ---------------------- Your implementation begins------------------------
    categories = product_processed_data.select(F.col(category_column))
    stringIndexer = M.feature.StringIndexer(
        inputCol=category_column,
        outputCol=categoryIndex_column,
    )
    cat_indexes = stringIndexer.fit(categories)
    indexed = cat_indexes.transform(categories)
    
    onehot = M.feature.OneHotEncoder(
        inputCol=categoryIndex_column,
        outputCol=categoryOneHot_column,
        dropLast=False
    )
    onehot_model = onehot.fit(indexed)
    onehot_data = onehot_model.transform(indexed)

    pca = M.feature.PCA(
        k=15,
        inputCol=categoryOneHot_column,
        outputCol = categoryPCA_column
    )
    pca_model = pca.fit(onehot_data)
    pca_data = pca_model.transform(onehot_data)
    
    count_total = pca_data.count()
    
    summarizer = M.stat.Summarizer()
    mean_row_onehot = pca_data.select(
        summarizer.mean(F.col(categoryOneHot_column)).alias("meanVector_categoryOneHot")
    ).first()
    meanVector_categoryOneHot = mean_row_onehot["meanVector_categoryOneHot"]
    
    mean_row_pca = pca_data.select(
        summarizer.mean(F.col(categoryPCA_column)).alias("meanVector_categoryPCA")
    ).first()
    meanVector_PCA = mean_row_pca["meanVector_categoryPCA"]

    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'count_total': None,
        'meanVector_categoryOneHot': [None, ],
        'meanVector_categoryPCA': [None, ]
    }
    # Modify res:
    res['count_total'] = count_total
    res['meanVector_categoryOneHot'] = meanVector_categoryOneHot
    res['meanVector_categoryPCA'] = meanVector_PCA



    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_6')
    return res
    # -------------------------------------------------------------------------
    
    
def task_7(data_io, train_data, test_data):
    # ---------------------- Your implementation begins------------------------

  # using the builtin function and settings
    
    label_col = 'overall' if 'overall' in train_data.columns else 'label'

    
    train_vector = train_data.select("features", label_col)
    test_vector = test_data.select("features", label_col)

    
    dt = DecisionTreeRegressor(featuresCol="features", labelCol=label_col, maxDepth=5)
    model = dt.fit(train_vector)

    predicts = model.transform(test_vector)

   
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
    test_rmse = evaluator.evaluate(predicts)

    
    # -------------------------------------------------------------------------

    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': test_rmse
    }
    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_7')
    return res
    # -------------------------------------------------------------------------
    
    
def task_8(data_io, train_data, test_data):
    
    # ---------------------- Your implementation begins------------------------
    
    label_col = 'overall' if 'overall' in train_data.columns else 'label'
    
    train_df, test_df = train_data.randomSplit([0.75,0.25], seed=SEED)
    train_vector = train_df.select("features", label_col)
    valid_vector = test_df.select("features", label_col)
    test_vector = test_data.select("features", label_col)
    
    best_model = None
    best_rmse = float('inf')
    rmses = []
        
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction", metricName="rmse")
    for i in [5, 7, 9, 12]:
        dt = DecisionTreeRegressor(featuresCol="features", labelCol=label_col, maxDepth=i)
        model = dt.fit(train_vector)
        predictions = model.transform(valid_vector)
        valid_rmse_depth = evaluator.evaluate(predictions)
        rmses.append(valid_rmse_depth)
        if valid_rmse_depth < best_rmse: 
            best_model = model
            best_rmse = valid_rmse_depth
        
    valid_rmse_depth_5, valid_rmse_depth_7, valid_rmse_depth_9, valid_rmse_depth_12 = rmses
    pred = best_model.transform(test_vector)
    test_rmse = evaluator.evaluate(pred)
    
    # -------------------------------------------------------------------------
    
    
    # ---------------------- Put results in res dict --------------------------
    res = {
        'test_rmse': None,
        'valid_rmse_depth_5': None,
        'valid_rmse_depth_7': None,
        'valid_rmse_depth_9': None,
        'valid_rmse_depth_12': None,
    }
    # Modify res:
    res['test_rmse'] = test_rmse
    res['valid_rmse_depth_5'] = valid_rmse_depth_5
    res['valid_rmse_depth_7'] = valid_rmse_depth_7
    res['valid_rmse_depth_9'] = valid_rmse_depth_9
    res['valid_rmse_depth_12'] = valid_rmse_depth_12

    # -------------------------------------------------------------------------

    # ----------------------------- Do not change -----------------------------
    data_io.save(res, 'task_8')
    return res
    # -------------------------------------------------------------------------