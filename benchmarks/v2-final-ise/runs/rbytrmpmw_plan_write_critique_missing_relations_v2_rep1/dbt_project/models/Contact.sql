{{ config(materialized='table') }}

SELECT
    TRIM(c.id) AS "Id",
    NULLIF(TRIM(SPLIT_PART(c.full_name, ' ', 1)), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(INITCAP(SPLIT_PART(c.full_name, ' ', 2))), ''), 'Unknown') AS "LastName",
    LOWER(TRIM(c.email)) AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    a.id AS "AccountId",
    TRIM(c.id) AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a
    ON TRIM(UPPER(c.account_ref)) = TRIM(UPPER(a.id))