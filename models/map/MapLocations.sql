SELECT LocationPK, CustomerID, CAST(ROW_NUMBER() OVER (PARTITION BY CustomerFK ORDER BY LocationPK) AS TinyInt) LocationID 
FROM {{ source('avi', 'Locations') }} l 
JOIN {{ ref('MapCustomers') }} c ON c.CustomerPK = l.CustomerFK
