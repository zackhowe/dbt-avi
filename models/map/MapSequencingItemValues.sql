select CustomerId, ItemId, ValueTypeId, PeriodBeginUtcDttm, case when LeadDttm is null then CAST('9999-12-31' AS DateTime) else LeadDttm end PeriodEndUtcDttm 
from (
select v.CustomerId, v.ItemId, v.ValueTypeId, v.PeriodBeginUtcDttm, lead(PeriodBeginUtcDttm, 1) OVER (PARTITION BY v.CustomerId, v.ItemId, v.ValueTypeId ORDER BY v.PeriodBeginUtcDttm) LeadDttm 
from {{ ref('TempItemValues') }} v
join (select customerid, itemid, ValueTypeID, count(*) dup_count from {{ ref('TempItemValues') }} group by customerid, itemid, ValueTypeID having count(*) > 1) dups
on v.customerid = dups.CustomerID and v.ItemId = dups.itemid and v.ValueTypeID = dups.ValueTypeID
) a
