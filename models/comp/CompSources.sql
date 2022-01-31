
SELECT CAST(row_number() OVER (ORDER BY ItemCompLocation) AS SmallInt) CompSourceID
, ItemCompLocation AS CompSource
FROM (
SELECT ItemCompLocation, count(*) dup_count from {{ source('inv', 'ItemComps') }}
WHERE ItemCompLocation NOT IN ('Free shipping', 'System', 'Old Appraisal', 'from 2 sellers', 'Previous Appraisal', 'from 3 sellers', 'Client', 'from 4 sellers', 'from 5 sellers')
GROUP BY ItemCompLocation HAVING count(*) > 1000
) agg
