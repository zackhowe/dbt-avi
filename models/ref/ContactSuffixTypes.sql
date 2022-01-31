SELECT ContactSuffixTypePK AS ContactSuffixTypeID, ContactSuffixTypeDesc
FROM {{ source('ref', 'ContactSuffixTypes') }}