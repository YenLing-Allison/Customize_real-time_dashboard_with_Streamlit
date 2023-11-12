DROP TABLE IF EXISTS purchase1;
CREATE TABLE purchase1 AS
SELECT 
    InventoryId, Store, Brand, Vendornumber, VendorName,
    CASE
        WHEN STR_TO_DATE(ReceivingDate, '%Y/%m/%d') BETWEEN '2016-01-01' AND '2016-01-07' THEN 1
        WHEN STR_TO_DATE(ReceivingDate, '%Y/%m/%d') BETWEEN '2016-01-08' AND '2016-01-14' THEN 2
        WHEN STR_TO_DATE(ReceivingDate, '%Y/%m/%d') BETWEEN '2016-01-15' AND '2016-01-21' THEN 3
        WHEN STR_TO_DATE(ReceivingDate, '%Y/%m/%d') BETWEEN '2016-01-22' AND '2016-01-28' THEN 4
        WHEN STR_TO_DATE(ReceivingDate, '%Y/%m/%d') BETWEEN '2016-01-29' AND '2016-02-04' THEN 5
        WHEN STR_TO_DATE(ReceivingDate, '%Y/%m/%d') BETWEEN '2016-02-05' AND '2016-02-11' THEN 6
        WHEN STR_TO_DATE(ReceivingDate, '%Y/%m/%d') BETWEEN '2016-02-12' AND '2016-02-18' THEN 7
        WHEN STR_TO_DATE(ReceivingDate, '%Y/%m/%d') BETWEEN '2016-02-19' AND '2016-02-25' THEN 8
        ELSE 9 END Week,
    SUM(Quantity) as PurchaseQuantity,
    SUM(Dollars) AS TotalPurchasePrice
FROM purchase_s5
GROUP BY 1,2,3,4,5,6;

DROP TABLE IF EXISTS sales1;
CREATE TABLE sales1 AS 
SELECT 
    InventoryId, Store, Brand, VendorNo, VendorName,
    CASE
        WHEN STR_TO_DATE(SalesDate, '%m/%d/%Y') BETWEEN '2016-01-01' AND '2016-01-07' THEN 1
        WHEN STR_TO_DATE(SalesDate, '%m/%d/%Y') BETWEEN '2016-01-08' AND '2016-01-14' THEN 2
        WHEN STR_TO_DATE(SalesDate, '%m/%d/%Y') BETWEEN '2016-01-15' AND '2016-01-21' THEN 3
        WHEN STR_TO_DATE(SalesDate, '%m/%d/%Y') BETWEEN '2016-01-22' AND '2016-01-28' THEN 4
        WHEN STR_TO_DATE(SalesDate, '%m/%d/%Y') BETWEEN '2016-01-29' AND '2016-02-04' THEN 5
        WHEN STR_TO_DATE(SalesDate, '%m/%d/%Y') BETWEEN '2016-02-05' AND '2016-02-11' THEN 6
        WHEN STR_TO_DATE(SalesDate, '%m/%d/%Y') BETWEEN '2016-02-12' AND '2016-02-18' THEN 7
        WHEN STR_TO_DATE(SalesDate, '%m/%d/%Y') BETWEEN '2016-02-19' AND '2016-02-25' THEN 8
        ELSE 9 END Week,
    SUM(SalesQuantity) AS SalesQuantity,
    SUM(SalesDollars) AS TotalSalesPrice
FROM sales_s5
GROUP BY 1,2,3,4,5,6;

DROP TABLE IF EXISTS beginv1;
CREATE TABLE beginv1 AS
SELECT 
    InventoryId
    , Store
    , Brand
    , 1 AS Week
    , onHand
    , onHand * Price AS TotalBegCost
FROM begininv_s5;

DROP TABLE IF EXISTS item_list;
CREATE TABLE item_list AS (
    SELECT InventoryId, Store, Brand, Week
    FROM purchase1
    UNION ALL
    SELECT InventoryId, Store, Brand, Week
    FROM sales1
    UNION ALL
    SELECT InventoryId, Store, Brand, Week
    FROM beginv1
);

-- And then the rest of your query here, referring to these temporary tables instead of the CTEs.
WITH beg_pur_sal AS (
    SELECT 
        a.*
        , COALESCE(b.PurchaseQuantity, 0) AS PurchaseQuantity
        , COALESCE(b.TotalPurchasePrice, 0) AS TotalPurchasePrice
        , COALESCE(c.SalesQuantity, 0) AS SalesQuantity
        , COALESCE(c.TotalSalesPrice, 0) AS TotalSalesPrice
        , COALESCE(d.onHand, 0) AS BegQuantity
        , COALESCE(d.TotalBegCost, 0) AS TotalBegCost
    FROM item_list a
    LEFT JOIN purchase1 b 
        ON a.InventoryId = b.InventoryId
        AND a.Store = b.Store
        AND a.Brand = b.Brand
--         AND a.Description = b.Description
        AND a.Week = b.Week
    LEFT JOIN sales1 c
        ON a.InventoryId = c.InventoryId
        AND a.Store = c.Store
        AND a.Brand = c.Brand
--         AND a.Description = c.Description
        AND a.Week = c.Week
    LEFT JOIN beginv1 d
        ON a.InventoryId = d.InventoryId
        AND a.Store = d.Store
        AND a.Brand = d.Brand
--         AND a.Description = d.Description
        AND a.Week = d.Week
)

,  qty_agg AS (
    SELECT
        InventoryId
        , Store
        , Brand
        , Week
        , TotalPurchasePrice
        , TotalSalesPrice
        , TotalBegCost
        , PurchaseQuantity
        , SalesQuantity
        , BegQuantity
        , SUM(PurchaseQuantity) OVER(PARTITION BY InventoryId, Store, Brand 
                ORDER BY Week
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS PurchaseQuantity_AGG
        , SUM(SalesQuantity) OVER(PARTITION BY InventoryId, Store, Brand 
                ORDER BY Week
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS SalesQuantity_AGG
        , SUM(BegQuantity) OVER(PARTITION BY InventoryId, Store, Brand 
                ORDER BY Week
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BegQuantity_AGG
    FROM beg_pur_sal
) 

, lag_inventory AS (
    SELECT
        a.InventoryId
        , a.Store
        , a.Brand
        , a.Week
        , b.PurchasePrice
        , a.TotalPurchasePrice
        , a.TotalSalesPrice
        , a.TotalBegCost
        , a.PurchaseQuantity
        , a.SalesQuantity
        , a.BegQuantity_AGG + a.PurchaseQuantity_AGG - a.SalesQuantity_AGG AS EndInv
        , LAG(a.BegQuantity_AGG + a.PurchaseQuantity_AGG - a.SalesQuantity_AGG) 
            OVER(PARTITION BY a.InventoryId, a.Store, a.Brand ORDER BY Week)
                AS BegInv
    FROM qty_agg a
    JOIN (
        SELECT Brand, PurchasePrice
        FROM purchaseprices 
    ) b ON a.Brand = b.Brand
)

SELECT *
FROM (
SELECT
    a.InventoryId
    , a.Store
    , a.Brand
    , a.Week
    , a.PurchasePrice
    , a.TotalPurchasePrice
    , a.TotalSalesPrice
    , a.TotalBegCost
    , a.PurchaseQuantity
    , a.SalesQuantity
    , COALESCE(a.BegInv, d.BegQuantity) AS BegInv
    , a.EndInv
    , a.PurchasePrice * a.SalesQuantity AS COGS
    , a.PurchasePrice * a.EndInv AS InventoryValue
    , (a.PurchasePrice * a.SalesQuantity) / ((a.EndInv + COALESCE(a.BegInv, d.BegQuantity)) / 2) AS InventoryTurnover
    , 52 / ((a.PurchasePrice * a.SalesQuantity) / ((a.EndInv + COALESCE(a.BegInv, d.BegQuantity)) / 2)) AS DOI
FROM lag_inventory a
LEFT JOIN beg_pur_sal d
    ON a.InventoryId = d.InventoryId
    AND a.Store = d.Store
    AND a.Brand = d.Brand
    AND a.Week = d.Week
) a
WHERE InventoryTurnover IS NOT NULL AND InventoryTurnover != 0