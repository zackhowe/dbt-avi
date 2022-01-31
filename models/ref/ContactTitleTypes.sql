SELECT ContactTitleTypePK AS ContactTitleTypeID, ContactTitleTypeDesc
FROM {{ source('ref', 'ContactTitleTypes') }}