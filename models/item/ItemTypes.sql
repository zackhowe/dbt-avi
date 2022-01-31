select ItemTypePK AS ItemTypeID, ItemTypeDesc, ItemCategoryFK AS ItemCategoryID, LowValue, HighValue, DefaultValue 
from {{ source('itm', 'ItemTypes') }}
