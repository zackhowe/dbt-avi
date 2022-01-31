SELECT CustomerID
--, CAST(null AS Timestamp) AS RowVersion
, Timestamp AS CreateUtcDttm
, LanguageFK AS LanguageId
, 5 AS CustomerStatusId --complete
, CustomerName
, Salutation
, ReferredBy
, Notes
, Password AS PdfPassword
, newid() as CustomerToken
FROM {{ source('avi', 'Customers') }} c JOIN {{ ref('MapCustomers') }} map ON map.CustomerPK = c.CustomerPK
