SELECT m1.CustomerID, m1.ItemID, m2.CustomerId AS CompCustomerID, m2.ItemID AS CompItemID, c.ItemCompDt AS CompLinkUtcDttm
FROM (
SELECT q.*, left(inv_dot_item, charindex('.', inv_dot_item)-1) CompInventoryPK, right(inv_dot_item, len(inv_dot_item) - charindex('.', inv_dot_item)) CompItemPK
FROM (SELECT c.*, RIGHT(ItemCompLocation, LEN(ItemCompLocation)-15) inv_dot_item FROM {{ source('inv', 'ItemComps') }} c WHERE ItemCompLocation LIKE 'Old Appraisal: %.%') q
) c
JOIN {{ ref('MapItems') }} m1 ON m1.InventoryPK = c.InventoryPK AND m1.ItemPK = c.ItemPK
JOIN {{ ref('MapItems') }} m2 ON m2.InventoryPK = c.CompInventoryPK AND m2.ItemPK = c.CompItemPK;
