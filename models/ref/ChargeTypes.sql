SELECT ChargeTypePK AS ChargeTypeID, ChargeTypeDesc
FROM {{ source('ref', 'ChargeTypes') }}