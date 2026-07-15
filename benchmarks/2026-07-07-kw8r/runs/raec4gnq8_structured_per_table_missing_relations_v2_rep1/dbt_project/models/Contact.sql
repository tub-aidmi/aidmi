{{ config(materialized='table') }}

SELECT 
    c.id AS "Id",
    TRIM(SPLIT_PART(c.full_name, ' ', 1)) AS "FirstName",
    TRIM(SPLIT_PART(c.full_name, ' ', 2)) AS "LastName",
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
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON c.account_ref = a.id OR c.company_name = a.name