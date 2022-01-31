select u.UserPK AS UserId
, u.UserName
, u.Timestamp AS CreateUtcDttm
, u.FullName
, u.Password
, u.CanDoFieldEntry
, u.CanDoAppraisal
, u.IsManager
, u.IsAdministrator
, u.IsDisabled
, u.LastLoginDateTime AS LastLoginDttm
, u.ClientVersion
FROM {{ source('avi', 'Users') }} u;

