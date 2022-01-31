SELECT LanguagePK AS LanguageID, LanguageDesc
FROM {{ source('ref', 'Languages') }}