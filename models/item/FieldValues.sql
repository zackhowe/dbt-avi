select FieldValuePK AS FieldValueID, FieldListFK AS FieldListID, FieldValueDesc 
FROM {{ source('itm', 'FieldValues') }}
