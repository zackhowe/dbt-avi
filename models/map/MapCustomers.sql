SELECT CustomerPK, CAST(ROW_NUMBER() OVER (ORDER BY CustomerPK) AS SmallInt) AS CustomerID
FROM {{ source('avi', 'Customers') }}
