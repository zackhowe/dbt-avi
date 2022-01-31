SELECT map.CustomerID
, map.ItemID
, inv.timestamp AS PeriodBeginUtcDttm
, CAST('9999-12-31' AS DateTime) AS PeriodEndUtcDttm
, mapr.LocationId
, mapr.RoomID
, mapc.CollectionID
, b.BeneficiaryID
, i.VacNumber
, i.VacValue
FROM {{ source('inv', 'Items') }} i
JOIN {{ ref('MapItems') }} map ON map.InventoryPK = i.InventoryPK AND map.ItemPK = i.ItemPK
JOIN {{ ref('MapRooms') }} mapr ON mapr.InventoryPK = i.InventoryPK AND mapr.RoomPK = i.RoomFK
LEFT JOIN {{ ref('MapCollections') }} mapc ON mapc.InventoryPK = i.InventoryPK AND mapc.CollectionPK = i.CollectionFK
LEFT JOIN {{ ref('Beneficiaries') }} b ON b.CustomerID = map.CustomerId AND b.BeneficiaryID = i.BeneficiaryFK
JOIN {{ source('avi', 'Inventories') }} inv ON inv.InventoryPK = i.InventoryPK
