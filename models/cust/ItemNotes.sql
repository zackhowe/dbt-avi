select CustomerID
, ItemID
, CAST('2000-01-01 00:00:00' AS DateTime) AS CreateUtcDttm
, CAST(null AS Timestamp) AS RowVersion
, Notes AS Note
, Cast(null AS TinyInt) AS AviUserID
, Cast(null AS nvarchar(128)) AS WebUserId
FROM {{ ref('MapItemsAgg') }} items
JOIN {{ ref('MapCustomers') }} mc on mc.CustomerPK = items.CustomerFK
WHERE Notes IS NOT NULL

UNION ALL

SELECT i.CustomerID
, i.ItemID
, timestamp
, null
, note
, Cast(null AS TinyInt) AS AviUserID
, Cast(null AS nvarchar(128)) AS WebUserId
FROM {{ source('inv', 'ItemNotes') }} n 
JOIN {{ ref ('MapItems') }} i ON i.InventoryPK = n.InventoryPK AND i.ItemPK = n.ItemPK

