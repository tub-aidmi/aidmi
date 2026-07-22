SELECT
    c.id AS "Id",
    CASE
        WHEN POSITION(' ' IN TRIM(c.full_name)) > 0 THEN SPLIT_PART(TRIM(c.full_name), ' ', 1)
        ELSE NULL
    END AS "FirstName",
    CASE
        WHEN c.full_name IS NULL OR TRIM(c.full_name) = '' THEN 'Unknown'
        WHEN POSITION(' ' IN TRIM(c.full_name)) = 0 THEN TRIM(c.full_name)
        ELSE SUBSTRING(TRIM(c.full_name) FROM POSITION(' ' IN TRIM(c.full_name)) + 1)
    END AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_missing_relations_v2_src', 'contact') }} AS c
LEFT JOIN
    {{ source('fixture_missing_relations_v2_src', 'account') }} AS a
    ON c.account_ref = a.id