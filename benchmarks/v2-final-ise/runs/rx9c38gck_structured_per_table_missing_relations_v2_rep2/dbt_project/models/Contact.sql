{{ config(materialized='table') }}

WITH src_contact AS (
    SELECT
        id,
        full_name,
        email,
        account_ref,
        company_name
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}
),
src_account AS (
    SELECT
        id,
        name
    FROM {{ source('fixture_missing_relations_v2_src', 'account') }}
)

SELECT
    c.id AS "Id",
    CASE 
        WHEN c.full_name ~ '^\s*\S+\s+\S+\s*$' 
        THEN TRIM(SPLIT_PART(TRIM(c.full_name), ' ', 1))
        ELSE NULL 
    END AS "FirstName",
    CASE 
        WHEN c.full_name ~ '^\s*\S+\s+\S+\s*$' 
        THEN TRIM(SPLIT_PART(TRIM(c.full_name), ' ', 2))
        ELSE TRIM(c.full_name)
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

FROM src_contact c
LEFT JOIN src_account a ON c.account_ref = a.id OR c.account_ref = a.name