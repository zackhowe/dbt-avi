with ItemHistory AS (
 SELECT ItemHistoryPK
 , ItemCategoryFK
 , TRIM(ItemHistoryKeyword) AS ItemHistoryKeyword
 , TRIM(ItemHistoryDescription) AS ItemHistoryDescription
 FROM {{ source('ref', 'ItemHistories') }}
 WHERE COALESCE(TRIM(ItemHistoryDescription), '') <> ''
)
SELECT row_number() over (order by o.ItemCategoryFK, ItemHistoryDescription) AS ItemHistoryId
, o.ItemCategoryFK AS ItemCategoryId
, o.ItemHistoryDescription
, (SELECT '{'+STRING_AGG('"'+ d.ItemHistoryKeyword+'"', ', ')+'}' as ItemHistoryKeywords
 FROM (SELECT distinct TRIM(a.ItemHistoryKeyword) ItemHistoryKeyword
  FROM ItemHistory a
  JOIN ItemHistory b ON a.ItemHistoryDescription = b.ItemHistoryDescription 
  AND a.ItemCategoryFK = b.ItemCategoryFK 
  WHERE a.ItemHistoryDescription = o.ItemHistoryDescription and a.ItemCategoryFK = o.ItemCategoryFK
 ) AS D
) AS ItemHistoryKeywords
FROM ItemHistory o
GROUP BY o.ItemCategoryFK, o.ItemHistoryDescription

UNION ALL

SELECT row_number() OVER (ORDER BY ItemCategoryFK, ItemHistory) + (SELECT MAX(ItemHistoryPK) FROM {{ source('ref', 'ItemHistories') }}) AS ItemHistoryId
, ItemCategoryFK as ItemCategoryId
, 'REVIEW' AS ItemHistoryKeyword
, ItemHistory AS ItemHistoryDescription
FROM 
(
SELECT DISTINCT i.ItemCategoryFk, TRIM(i.ItemHistory) AS ItemHistory
FROM {{ source('inv', 'Items') }} i
WHERE COALESCE(TRIM(i.ItemHistory), '') <> ''
AND NOT EXISTS (
 SELECT 1 FROM {{ source('ref', 'ItemHistories') }} h
 WHERE TRIM(h.ItemHistoryDescription) = TRIM(i.ItemHistory)
 AND h.ItemCategoryFK = i.ItemCategoryFK
)
) AS AddItemHistories
;
