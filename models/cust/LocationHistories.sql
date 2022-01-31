with Inventories as (
select i.*, map_i.CustomerId
,DateAdd(SECOND, row_number() over (partition by map_i.CustomerId order by i.InventoryPk), i.CreateDt) AS AdjustedCreate
from {{ source('avi', 'Inventories') }} i
JOIN {{ ref('MapInventories') }} map_i ON map_i.InventoryPK = i.InventoryPK 
)
SELECT i.CustomerID
, map_l.LocationID
, i.AdjustedCreate AS PeriodBeginUtcDttm
, COALESCE(LEAD(AdjustedCreate) OVER (PARTITION BY i.CustomerID, map_l.LocationID ORDER BY i.CreateDt), CAST( '9999-12-31' as DateTime)) AS PeriodEndUtcDttm
, CAST(NULL AS Int) AS ImageFileID
, i.SeniorAppraiserFK AS SeniorAppraiserID
, i.AssistantAppraiserFK AS AssistantAppraiserID
, i.AppointmentDt
, i.EffectiveDt
, i.SystemValuesDt
, i.ChargeTypeFK AS ChargeTypeID
, i.RateAmt
, i.LineItemAmt
, i.CertifiedAmt
, i.PremiumAmt
, i.MinimumReportValue
, i.ContentCoverageAmt
, i.GlobalBlanketPolicyAmt
, i.AdjustmentSalesTaxPct
, i.AdjustmentShippingPct
, i.AdjustmentDecoratorPct
, i.AdjustmentInCalculations
, i.InventoryOverrideAmt AS LocationOverrideAmt
FROM Inventories i 
JOIN {{ ref('MapLocations') }} map_l ON map_l.LocationPK = i.LocationFK 
