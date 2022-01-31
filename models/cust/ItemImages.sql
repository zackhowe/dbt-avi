select i.CustomerID
, i.ItemID
, ImageFileId 
from {{ source('inv', 'ItemImages') }} ii 
join {{ ref('MapItems') }} i ON i.InventoryPK = ii.InventoryPK AND i.ItemPK = ii.ItemPK 
join {{ ref('MapImageFiles') }} img ON img.InventoryPK = ii.InventoryPK AND img.ImageFilePK = ii.ImageFilePK;
