select CustomerID
, CAST(BeneficiaryPK AS TinyInt) AS BeneficiaryID
, CAST(1 as Bit) AS ServerIdFlag
, CAST(0 as Bit) AS SoftDeleteFlag
, CAST(null as TIMESTAMP) AS RowVersion
, CAST(timestamp as DateTime) AS CreateUtcDttm
, relationshipfk AS BeneficiaryTypeID
, BeneficiaryName 
, Address
, City
, State
, Zip 
from {{ source('ref', 'Beneficiaries') }} b 
join {{ ref('MapCustomers') }} c on c.CustomerPK = b.CustomerFK
