SELECT RelationshipPK AS BeneficiaryTypeID, Relationship AS BeneficiaryTypeDesc
FROM {{ source('ref', 'BeneficiaryRelationships') }}