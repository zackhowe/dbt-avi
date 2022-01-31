SELECT ROW_NUMBER() OVER (ORDER BY MinItemCompDt) CompItemID
, CAST(1 as Bit) ServerIdFlag
, CAST(null AS Timestamp) RowVersion
, CURRENT_TIMESTAMP AS CreateUtcDttm
, CAST(NULL as TinyInt) AS UserID
, CAST(0 as TinyInt) AS ItemCategoryID
, CAST(null AS NVARCHAR(255)) AS ItemType
, MinItemCompDt AS CompDt
, ItemCompAmt AS CompAmt
, cs.CompSourceID
, CASE WHEN cs.CompSourceID IS NULL THEN ItemCompLocation END AS CompSource
, ItemCompDesc AS CompDescription
, NULL AS CompFields
FROM (
SELECT ItemCompLocation, ItemCompDesc, ItemCompAmt, MIN(ItemCompDt) MinItemCompDt 
FROM {{ source('inv', 'ItemComps') }}
WHERE ItemCompLocation NOT LIKE 'Old Appraisal: %.%' 
GROUP BY ItemCompLocation, ItemCompDesc, ItemCompAmt
) comp
LEFT JOIN {{ ref('CompSources') }} cs on cs.CompSource = ItemCompLocation;
