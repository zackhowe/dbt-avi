{{ config(materialization="ephemeral") }}

SELECT InventoryPK, ItemPK, 
(SELECT FieldKey AS [Key], FieldValue AS [Value] 
 FROM {{ source('inv', 'ItemFields') }} i 
 WHERE i.InventoryPK = f.InventoryPK AND i.ItemPK = f.ItemPK 
 ORDER BY FieldKey
 FOR JSON PATH
) AS ItemFields
FROM {{ source('inv', 'ItemFields') }} f
GROUP BY InventoryPK, ItemPK
