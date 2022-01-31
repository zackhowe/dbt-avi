SELECT ContactTypePK AS ContactTypeID, ContactTypeDesc
FROM {{ source('ref', 'ContactTypes') }}