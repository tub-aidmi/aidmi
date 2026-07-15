{{ config(materialized='table') }}

SELECT 
    c.id AS "Id",
    CASE 
        WHEN POSITION(' ' IN c.full_name) > 0 
        THEN SUBSTRING(c.full_name, 1, POSITION(' ' IN c.full_name) - 1)
        ELSE NULL 
    END AS "FirstName",
    CASE 
        WHEN POSITION(' ' IN c.full_name) > 0 
        THEN SUBSTRING(c.full_name, POSITION(' ' IN c.full_name) + 1)
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