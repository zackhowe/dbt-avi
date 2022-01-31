SELECT InsuranceCompanyPK AS InsuranceCompanyID, InsuranceCompanyDesc AS InsuranceCompanyDesc
FROM {{ source('ref', 'InsuranceCompanies') }}