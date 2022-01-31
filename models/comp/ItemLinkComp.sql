SELECT DISTINCT im.CustomerID, im.ItemID, ci.CompItemId, ic.ItemCompDt AS CompLinkUtcDttm
FROM {{ source('inv', 'ItemComps') }} ic 
JOIN {{ source('avi', 'Inventories') }} i ON i.InventoryPK = ic.InventoryPK 
JOIN {{ source('avi', 'Locations') }} l ON l.LocationPK = i.LocationFK
JOIN {{ ref('MapItems') }} im ON im.InventoryPK = ic.InventoryPK AND im.ItemPK = ic.InventoryPK
JOIN {{ ref('CompItems') }} ci ON ci.CompAmt = ic.ItemCompAmt AND ci.CompDescription = ic.ItemCompDesc AND (ci.CompSource IS NULL OR ci.CompSource = ic.ItemCompLocation)
LEFT JOIN {{ ref('CompSources') }} cs ON cs.CompSourceID = ci.CompSourceID AND cs.CompSource = ic.ItemCompLocation
WHERE ic.ItemCompLocation NOT LIKE 'Old Appraisal: %.%'
