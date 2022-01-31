{{
  config(
    post_hook=["DROP TABLE {{ ref('TempItemValues') }}", "DROP TABLE {{ ref('MapSequencingItemValues') }}"]
  )
}}
SELECT i.CustomerID
, i.ItemID
, i.ValueTypeID
, i.PeriodBeginUtcDttm
, COALESCE(seq.PeriodEndUtcDttm, i.PeriodEndUtcDttm) AS PeriodEndUtcDttm
, i.AppraisedDt
, i.AppraiserUserID
, i.AppraisedValue
, i.ReportValueOverride
, i.AppraisalTypeID
, i.AppraisalTypeEnabled
FROM {{ ref('TempItemValues') }} i
LEFT JOIN {{ ref('MapSequencingItemValues') }} seq 
 ON seq.CustomerID = i.CustomerID AND seq.ItemID = i.ItemID AND seq.ValueTypeID = i.ValueTypeID AND seq.PeriodBeginUtcDttm = i.PeriodBeginUtcDttm