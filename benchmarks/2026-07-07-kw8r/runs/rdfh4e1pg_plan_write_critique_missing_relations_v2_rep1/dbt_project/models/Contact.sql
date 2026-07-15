{{ config(materialized='table') }}

WITH contact_raw AS (
    SELECT id, full_name, email, account_ref
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
account_raw AS (
    SELECT id
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)

SELECT 
    cr.id AS "Id",
    CASE 
        WHEN POSITION(' ' IN COALESCE(TRIM(cr.full_name), '')) > 0 
        THEN INITCAP(SUBSTR(COALESCE(TRIM(cr.full_name), ''), 1, POSITION(' ' IN COALESCE(TRIM(cr.full_name), '')) - 1))
        ELSE 'Unknown'
    END AS "FirstName",
    CASE 
        WHEN POSITION(' ' IN COALESCE(TRIM(cr.full_name), '')) > 0 
        THEN INITCAP(SUBSTR(COALESCE(TRIM(cr.full_name), ''), POSITION(' ' IN COALESCE(TRIM(cr.full_name), '')) + 1))
        ELSE 'Unknown'
    END AS "LastName",
    LOWER(TRIM(cr.email)) AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    ar.id AS "AccountId",
    cr.id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM contact_raw cr
LEFT JOIN account_raw ar 
    ON UPPER(TRIM(cr.account_ref)) = UPPER(TRIM(ar.id))