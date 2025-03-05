-- Total amount sales
SELECT
    SUM(total_amount) AS 'total_amount_sales'
FROM psychobunny_db.transactions
WHERE total_amount >= 0
;

-- Total amount refunded
SELECT
    SUM(total_amount) AS 'total_amount_refunds'
FROM psychobunny_db.transactions
WHERE total_amount < 0
;

-- Quantity items sold
SELECT
    productcode AS 'product_code',
    SUM(quantityordered) AS 'quantity_items_sold'
FROM psychobunny_db.transactions
WHERE total_amount > 0
GROUP BY productcode
;

-- Quantity items refunded
SELECT
    productcode AS 'product_code',
    SUM(quantityordered) AS 'quantity_items_refunded'
FROM psychobunny_db.transactions
WHERE total_amount < 0
GROUP BY productcode
;

-- Price per item
SELECT
    productcode AS 'product_code',
    SUM(total_amount) / SUM(quantityordered) AS 'item_price_sold'
FROM psychobunny_db.transactions
WHERE total_amount > 0
GROUP BY productcode
;

-- Top 10 customers
SELECT
    customername,
    SUM(total_amount)
FROM psychobunny_db.transactions
WHERE total_amount > 0
GROUP BY customername
ORDER BY SUM(total_amount) DESC
LIMIT 10;