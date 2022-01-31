SELECT ItemCategoryPK AS ItemCategoryID, ItemCategoryDesc, AdjustmentDecoratorFlag
FROM {{ source('ref', 'ItemCategories') }}