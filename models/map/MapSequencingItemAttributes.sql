SELECT CustomerId, ItemId, PeriodBeginUtcDttm, case when LeadDttm is null then CAST('9999-12-31' AS DateTime) else LeadDttm end PeriodEndUtcDttm
FROM (
SELECT a.CustomerId, a.ItemId, a.PeriodBeginUtcDttm, lead(PeriodBeginUtcDttm, 1) OVER (PARTITION BY a.CustomerId, a.ItemId ORDER BY a.PeriodBeginUtcDttm) LeadDttm 
FROM {{ ref('TempItemAttributes') }} a
JOIN (SELECT customerid, itemid, count(*) dup_count from {{ ref('TempItemAttributes') }} GROUP BY customerid, itemid having count(*) > 1) dups
ON a.customerid = dups.CustomerID AND a.ItemId = dups.itemid
) a
