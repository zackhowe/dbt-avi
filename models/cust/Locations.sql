SELECT m.CustomerID
, m.LocationID
, CAST(null as Timestamp) AS RowVersion
, l.Timestamp AS CreateUtcDttm
, l.LocationTypeFK AS LocationTypeID
, cast(null AS Int) AS ImageFileID
, l.LocationName
, l.AddressLine1
, l.AddressLine2
, l.City
, l.State
, l.Zip
, l.SquareFootage
, l.InsuranceCompanyFK as InsuranceCompanyID
, l.InsuranceAgentName
, l.InsuranceAgentPhone
, l.Notes 
, CAST(0 AS TINYINT) AS ValueTypeFlags
FROM {{ source('avi', 'Locations') }} l JOIN {{ ref('MapLocations') }} m ON m.LocationPK = l.LocationPK
