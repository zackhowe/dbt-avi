SELECT LocationTypePK AS LocationTypeID, LocationTypeDesc
FROM {{ source('ref', 'LocationTypes') }}