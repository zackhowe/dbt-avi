select x.UserGroupPK AS UserGroupId, GroupName
FROM {{ source('avi', 'UserGroups') }} x
