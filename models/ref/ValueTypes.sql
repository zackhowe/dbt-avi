SELECT InventoryTypePK AS ValueTypeID, CASE InventoryTypePK 
	WHEN 0 THEN 0
	WHEN 1 THEN 1 
	WHEN 2 THEN 2
	WHEN 3 THEN 4
	WHEN 4 THEN 8
	WHEN 5 THEN 16
	ELSE NULL
	END AS ValueTypeFlag, InventoryTypeDesc AS ValueTypeDesc
 , RptValueDesc, RptPurposeDesc, RptIntendedUseDesc
FROM {{ source('ref', 'InventoryTypes') }}