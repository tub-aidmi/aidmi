{{ config(materialized='table') }}

WITH contact_stg AS (
    SELECT 
        id,
        full_name,
        email,
        account_ref
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
)

SELECT 
    c.id AS "Id",
    CASE 
        WHEN TRIM(COALESCE(c.full_name, '')) = '' THEN NULL
        WHEN POSITION(' ' IN TRIM(c.full_name)) > 0 
            THEN INITCAP(SPLIT_PART(TRIM(c.full_name), ' ', 1))
        ELSE NULL
    END AS "FirstName",
    CASE 
        WHEN TRIM(COALESCE(c.full_name, '')) = '' THEN 'Unknown'
        WHEN POSITION(' ' IN TRIM(c.full_name)) > 0 
            THEN INITCAP(TRIM(SUBSTR(TRIM(c.full_name), LENGTH(SPLIT_PART(TRIM(c.full_name), ' ', 1)) + 2)))
        ELSE INITCAP(TRIM(c.full_name))
    END AS "LastName",
    LOWER(TRIM(COALESCE(c.email, ''))) AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_stg c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(COALESCE(c.account_ref, '')) = TRIM(a.id)