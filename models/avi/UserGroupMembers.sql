select x.UserPK AS UserId, x.UserGroupPK AS UserGroupId
FROM {{ source('avi', 'UsersGroups') }} x
