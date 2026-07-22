{{ config(materialized='table') }}

SELECT
    CAST(c."id" AS TEXT) AS "Id",
    INITCAP(
        CASE
            WHEN c."full_name" IS NULL THEN NULL
            WHEN POSITION(' ' IN c."full_name") > 0
                THEN LEFT(c."full_name", POSITION(' ' IN c."full_name") - 1)
            ELSE c."full_name"
        END
    ) AS "FirstName",
    INITCAP(
        CASE
            WHEN c."full_name" IS NULL THEN ''
            WHEN POSITION(' ' IN c."full_name") > 0
                THEN TRIM(SUBSTRING(c."full_name" FROM POSITION(' ' IN c."full_name") + 1))
            ELSE c."full_name"
        END
    ) AS "LastName",
    INITCAP(TRIM(c."email")) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    a."id" AS "AccountId",
    CAST(c."id" AS TEXT) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON c."account_ref" = a."id"