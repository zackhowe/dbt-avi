{{
  config(
    post_hook=["DROP TABLE {{ ref('TempItemAttributes') }}", "DROP TABLE {{ ref('MapSequencingItemAttributes') }}"]
  )
}}
SELECT i.CustomerID
, i.ItemID
, i.PeriodBeginUtcDttm
, COALESCE(seq.PeriodEndUtcDttm, i.PeriodEndUtcDttm) AS PeriodEndUtcDttm
, i.LocationId
, i.RoomID
, i.CollectionID
, i.BeneficiaryID
, i.VacNumber
, i.VacValue
FROM {{ ref('TempItemAttributes') }} i
LEFT JOIN {{ ref('MapSequencingItemAttributes') }} seq ON seq.CustomerID = i.CustomerID AND seq.ItemID = i.ItemID AND seq.PeriodBeginUtcDttm = i.PeriodBeginUtcDttm
