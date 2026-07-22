{{ config(materialized='table') }}

SELECT 
    c.id AS "Id",
    CASE 
        WHEN c.full_name IS NULL OR c.full_name = '' THEN NULL
        WHEN c.full_name ~ ' ' THEN 
            SUBSTRING(c.full_name FROM 1 FOR POSITION(' ' IN REVERSE(c.full_name)) - 1)
        ELSE NULL 
    END AS "FirstName",
    CASE 
        WHEN c.full_name IS NULL OR c.full_name = '' THEN 'Unknown'
        WHEN c.full_name ~ ' ' THEN 
            SUBSTRING(c.full_name FROM POSITION(' ' IN REVERSE(c.full_name)) + 1)
        ELSE c.full_name 
    END AS "LastName",
    c.email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    c.account_ref AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
