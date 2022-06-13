-- Big project for SQL
-- Dataset: Google Analytics Sample
-- Table Schema: https://support.google.com/analytics/answer/3437719?hl=en


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
SELECT
    LEFT(date,6) as month,
    COUNT(visitnumber)as visits,
    SUM(totals.pageviews) as pageviews,
    SUM(totals.transactions) as transactions,
    SAFE_DIVIDE(SUM(totals.totalTransactionRevenue),power(10,6)) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY month


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
    DISTINCT trafficsource.source,
    SUM(totals.visits) as total_visits,
    SUM(totals.bounces) as total_no_of_bounces,
    SAFE_DIVIDE(SUM(totals.bounces),SUM(totals.visits))*100 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficsource.source
ORDER BY total_visits DESC


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
with f_date as (
SELECT
    totals.totalTransactionRevenue as revenue,
    trafficsource.source as source,
    date,
    PARSE_DATE("%Y%m%d",date) as full_date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
)

SELECT
    'week' as time_type,
    CONCAT(EXTRACT(year from full_date),EXTRACT(week from full_date)) as time,
    f_date.source,
    SUM(f_date.revenue)/1000000 as revenue
FROM f_date
GROUP BY f_date.source, time

UNION ALL

SELECT
    'month' as time_type,
    LEFT(date,6) as time,
    f_date.source,
    SUM(f_date.revenue)/1000000 as revenue
FROM f_date
GROUP BY f_date.source, time


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with pv_purchase as (
SELECT
    LEFT(date,6) as month,
    ROUND(SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid),7) as avg_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170601' AND '20170731'
AND totals.transactions >=1
GROUP BY month
),

pv_nonpurchase as
(SELECT
   LEFT(date,6) as month,
   ROUND(SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid),7) as avg_pageviews_nonpurchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix BETWEEN '20170601' AND '20170731'
AND totals.transactions IS NULL
GROUP BY month)

SELECT
    pv_purchase.month,
    pv_purchase.avg_pageviews_purchase,
    pv_nonpurchase.avg_pageviews_nonpurchase
FROM pv_purchase JOIN pv_nonpurchase
USING(month)
ORDER BY pv_purchase.month



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT
    LEFT(date,6) as month,
    SUM(totals.transactions)/COUNT(DISTINCT fullvisitorid) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >=1
GROUP BY month

-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
    LEFT(date,6) as month,
    SUM(totals.totaltransactionrevenue)/SUM(totals.visits) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL
GROUP BY month


-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
with customers as (
SELECT 
    fullvisitorid as customerid
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) as hits, 
UNNEST(hits.product) as hitsproduct
WHERE hitsproduct.v2productname = "YouTube Men's Vintage Henley"
    AND totals.transactions >=1
    AND hitsproduct.productrevenue IS NOT NULL
)

SELECT
    hitsproduct.v2productname as other_purchased_products, 
    sum(hitsproduct.productquantity) as quantity
FROM customers JOIN `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
ON customers.customerid = `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`.fullvisitorid,
UNNEST(hits) as hits, 
UNNEST(hits.product) as hitsproduct
WHERE totals.transactions >=1
    AND hitsproduct.productrevenue IS NOT NULL
    AND hitsproduct.v2productname != "YouTube Men's Vintage Henley"
GROUP BY hitsproduct.v2productname
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with product_data as (
SELECT
FORMAT_DATE('%Y%m', parse_date('%Y%m%d',date)) as month,
COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
COUNT(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST(hits) as hits,
UNNEST (hits.product) as product
WHERE _table_suffix between '20170101' and '20170331'
AND eCommerceAction.action_type in ('2','3','6')
GROUP BY month
ORDER BY month
)

SELECT *,
ROUND(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
ROUND(num_purchase/num_product_view * 100, 2) as purchase_rate
FROM product_data
