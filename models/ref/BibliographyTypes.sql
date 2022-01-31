SELECT BibliographyTypePK AS BibliographyTypeID, BibliographyTypeDesc
FROM {{ source('ref', 'BibliographyTypes') }}