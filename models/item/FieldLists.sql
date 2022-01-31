SELECT FieldListPK AS FieldListID, FieldListDesc 
FROM {{ source('itm', 'FieldLists') }}
