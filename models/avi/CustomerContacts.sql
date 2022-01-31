SELECT CustomerID
, CustomerContactPK AS CustomerContactID
, CAST(null as Timestamp) AS RowVersion
, Timestamp AS CreateUtcDttm
, ContactTypeFK AS ContactTypeID
, ContactTitleTypeFK AS ContactTitleTypeID
, ContactSuffixTypeFK AS ContactSuffixTypeID
, ContactFirstName
, ContactMiddleName
, ContactLastName
, ContactPhone
, ContactEmail
FROM {{ source('avi', 'CustomerContacts') }} c JOIN {{ ref('MapCustomers') }} map ON map.CustomerPK = c.CustomerPK
