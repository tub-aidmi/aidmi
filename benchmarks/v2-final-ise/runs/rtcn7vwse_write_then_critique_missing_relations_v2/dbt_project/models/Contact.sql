{{ config(materialized='table') }}
SELECT 
    c."id" AS "Id",
    CASE 
        WHEN COALESCE(NULLIF(TRIM(c."full_name"), ''), '') = '' THEN NULL
        WHEN POSITION(' ' IN c."full_name") > 0 THEN SUBSTRING(c."full_name" FROM 1 FOR POSITION(' ' IN c."full_name") - 1)
        ELSE NULL
    END AS "FirstName",
    CASE 
        WHEN COALESCE(NULLIF(TRIM(c."full_name"), ''), '') = '' THEN NULL
        WHEN POSITION(' ' IN c."full_name") > 0 THEN SUBSTRING(c."full_name" FROM POSITION(' ' IN c."full_name") + 1)
        ELSE c."full_name"
    END AS "LastName",
    c."email" AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a."id" AS "AccountId",
    c."id" AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON c."account_ref" = a."id"