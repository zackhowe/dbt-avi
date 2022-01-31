SELECT BibliographyPK AS BibliographyID
, ItemCompLocation
, BibliographyAddress
, BibliographyTypeFK AS BibliographyTypeId
, AlwaysIncludeFlag
FROM {{ source('ref', 'Bibliographies') }}