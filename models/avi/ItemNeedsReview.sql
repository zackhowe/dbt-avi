SELECT DISTINCT map.CustomerId, map.ItemID, i.NeedsReview
FROM {{ source('inv', 'Items') }} i
JOIN {{ ref('MapItems') }} map ON map.InventoryPK = i.InventoryPK AND map.ItemPK = i.ItemPK
WHERE COALESCE(i.NeedsReview, '') <> ''
