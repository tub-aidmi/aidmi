{{ config(materialized='table') }}

SELECT
    TRIM(UPPER(id)) AS "Id",
    -- FirstName: text before the first space in full_name
    CASE
        WHEN full_name IS NOT NULL AND POSITION(' ' IN full_name) > 0
            THEN INITCAP(TRIM(LEFT(full_name, POSITION(' ' IN full_name) - 1)))
        WHEN full_name IS NOT NULL AND TRIM(full_name) != ''
            THEN INITCAP(TRIM(full_name))
        ELSE NULL
    END AS "FirstName",
    -- LastName: text after the first space; default 'Unknown' if no space or name is missing
    CASE
        WHEN POSITION(' ' IN COALESCE(full_name, '')) > 0
            THEN INITCAP(TRIM(SUBSTR(full_name, POSITION(' ' IN full_name) + 1)))
        ELSE 'Unknown'
    END AS "LastName",
    -- Email: lowercase and trimmed
    LOWER(TRIM(email)) AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    -- Role__c: no source column; fallback to NULL per enum policy
    NULL::TEXT AS "Role__c",
    -- Preferred_Language__c: no source language column; default EN
    'EN' AS "Preferred_Language__c",
    -- AccountId: normalized account_ref (ACC-XXXX format matches Account.Id directly)
    CASE WHEN account_ref IS NOT NULL THEN TRIM(account_ref) ELSE NULL END AS "AccountId",
    -- Legacy_Contact_ID__c: raw source id as-is
    TRIM(id) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}