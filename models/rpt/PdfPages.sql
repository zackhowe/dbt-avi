SELECT InventoryTypePK AS ValueTypeId, PageHeader, PageRtf
FROM {{ source('rpt', 'PdfPages') }}