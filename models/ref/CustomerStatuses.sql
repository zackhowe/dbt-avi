SELECT InventoryStatusPK AS CustomerStatusID, InventoryStatusDesc AS CustomerStatusDesc
FROM {{ source('ref', 'InventoryStatuses') }}