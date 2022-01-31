SELECT map.CustomerID
, map.ItemID
, inv.InventoryTypeFK ValueTypeID
, inv.Timestamp PeriodBeginUtcDttm
, CAST('9999-12-31' as DateTime) AS PeriodEndUtcDttm
, i.AppraisedDt
, i.AppraisedByUserFK AS AppraiserUserID
, i.AppraisedValue
, i.ReportValueOverride
, COALESCE(i.AppraisalTypeFK, 0) AS AppraisalTypeID
, i.AppraisalTypeEnabled
FROM {{ source('inv', 'Items') }} i
JOIN {{ ref('MapItems') }} map on map.InventoryPK = i.InventoryPK and map.ItemPK = i.ItemPK
JOIN {{ ref('MapRooms') }} mapr on mapr.InventoryPK = i.InventoryPK and mapr.RoomPK = i.RoomFK
LEFT JOIN {{ ref('MapCollections') }} mapc on mapc.InventoryPK = i.InventoryPK and mapc.CollectionPK = i.CollectionFK
LEFT JOIN {{ ref('Beneficiaries') }} b ON b.CustomerID = map.CustomerId and b.BeneficiaryID = i.BeneficiaryFK
JOIN {{ source('avi', 'Inventories') }} inv on inv.InventoryPK = i.InventoryPK
