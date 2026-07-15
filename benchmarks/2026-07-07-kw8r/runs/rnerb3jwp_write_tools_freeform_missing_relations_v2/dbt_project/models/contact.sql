{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    CASE 
        WHEN POSITION(' ' IN COALESCE(c.full_name, '')) > 0
        THEN TRIM(SUBSTRING(COALESCE(c.full_name, '') FROM 1 FOR POSITION(' ' IN c.full_name) - 1))
        ELSE ''
    END AS "FirstName",
    CASE 
        WHEN POSITION(' ' IN COALESCE(c.full_name, '')) > 0
        THEN TRIM(SUBSTRING(COALESCE(c.full_name, '') FROM POSITION(' ' IN c.full_name) + 1))
        ELSE COALESCE(c.full_name, 'Unknown')
    END AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    company_name AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    acc.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc
    ON c.account_ref = acc.id
