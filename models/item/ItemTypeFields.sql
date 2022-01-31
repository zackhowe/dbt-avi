select ItemTypeFieldPK AS ItemTypeFieldID, ItemTypeFK as ItemTypeID, FieldDesc, FieldListFK AS FieldListID
from {{ source('itm', 'ItemTypeFields') }}
