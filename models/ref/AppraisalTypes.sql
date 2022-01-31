SELECT AppraisalTypePK AS AppraisalTypeID, AppraisalTypeDesc, AppraisalTypeBaseValue
FROM {{ source('ref', 'AppraisalTypes') }}