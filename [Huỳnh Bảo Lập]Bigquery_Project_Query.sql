-- Big project for SQL

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT
    FORMAT_DATE("%Y%m", parse_date('%Y%m%d', DATE)) MONTH,
    SUM(totals.visits) AS visits,
    SUM(totals.pageviews) AS pageviews,
    SUM(totals.transactions) AS transactions,
    SUM(totals.totaltransactionrevenue)/POWER(10,6) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY 1
ORDER BY 1 ASC;

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT  
    trafficSource.source AS  source, 
    COUNT(trafficSource.source) AS total_visits,
    COUNT(totals.bounces) AS total_no_of_bounces,
    ROUND((COUNT(totals.bounces) / COUNT(trafficSource.source))*100,8) AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE trafficSource.source IN ('google','(direct)','youtube.com','analytics.google.com')
GROUP BY trafficSource.source
ORDER BY COUNT(trafficSource.source) DESC;

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
WITH month_num AS
(SELECT 
    'Month' time_type,
    FORMAT_DATE("%Y%m", parse_date('%Y%m%d', DATE)) AS time,
    trafficSource.source AS  source, 
    SUM(totals.totalTransactionRevenue)/POWER(10,6) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
 AND trafficSource.source IN ('(direct)','google')
GROUP BY 2,3),

week_num AS
(SELECT 
    'Week' time_type,
    FORMAT_DATE("%Y%V", parse_date('%Y%m%d', DATE)) AS time,
    trafficSource.source AS  source, 
    SUM(totals.totalTransactionRevenue)/POWER(10,6) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
 AND trafficSource.source IN ('(direct)')
 AND FORMAT_DATE("%Y%V", parse_date('%Y%m%d', DATE)) IN ('201724','201725')
GROUP BY 2,3)

SELECT * FROM month_num
UNION ALL 
SELECT * FROM week_num 
ORDER BY 4 DESC;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH non_purchase_num AS
(SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', DATE)) AS month,
    ROUND(SUM(totals.pageviews) / COUNT(DISTINCT fullvisitorid),9) AS purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE totals.transactions IS NULL
 AND _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
GROUP BY 1),

purchase_num AS 
(SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', DATE)) AS month,
    ROUND(SUM(totals.pageviews) / COUNT(DISTINCT fullvisitorid),9) AS non_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE totals.transactions >=1
 AND _TABLE_SUFFIX BETWEEN '20170601' AND '20170731'
GROUP BY 1)

SELECT * 
FROM purchase_num 
JOIN non_purchase_num 
 USING(month)
ORDER BY 1;

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', DATE)) AS month,
    ROUND(SUM(totals.transactions) / COUNT(DISTINCT fullvisitorid),9) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >=1
GROUP BY 1

-- Query 06: Average amount of money spent per session
#standardSQL
SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', DATE)) AS month,
    CEILING(SUM(totals.totalTransactionRevenue) / COUNT(fullvisitorid)) AS avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL  
GROUP BY 1;

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
SELECT 
        product.V2productName AS other_purchased_products, 
        SUM(product.productquantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
WHERE 
    fullVisitorId IN 
        (SELECT DISTINCT fullVisitorId
         FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
                UNNEST(hits) AS hits,
                UNNEST(hits.product) AS product
         WHERE product.V2productName = "YouTube Men's Vintage Henley"
         AND product.productrevenue IS NOT NULL)
    AND product.productrevenue IS NOT NULL 
    AND product.V2productName IN ("Google Sunglasses",
                                "Google Women's Vintage Hero Tee Black",
                                "SPF-15 Slim & Slender Lip Balm",
                                "Google Women's Short Sleeve Hero Tee Red Heather")
GROUP BY 1
ORDER BY 2 DESC;

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
with view_product_page as
(SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', DATE)) AS month,
    COUNT(hits.ecommerceaction.action_type) AS num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
 AND hits.ecommerceaction.action_type ='2'
GROUP BY 1
ORDER BY 1),

add_to_cart AS
(SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', DATE)) AS month,
    COUNT(hits.ecommerceaction.action_type) AS num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
 AND hits.ecommerceaction.action_type ='3'
GROUP BY 1
ORDER BY 1),

purchase AS
(SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', DATE)) AS month,
    COUNT(hits.ecommerceaction.action_type) AS num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
 AND hits.ecommerceaction.action_type ='6'
 AND product.v2productname IS NOT NULL
GROUP BY 1
ORDER BY 1)

SELECT 
    month,
    num_product_view, 
    num_addtocart,
    num_purchase,
    ROUND((num_addtocart / num_product_view)*100,2) AS add_to_cart_rate,
    ROUND((num_purchase / num_product_view)*100,2) AS purchase_rate
FROM view_product_page
JOIN add_to_cart
 USING(month)
JOIN purchase
 USING(month)
ORDER BY 1 ASC;
