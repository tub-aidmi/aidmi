{{ config(materialized='table') }}

SELECT 
    c.id AS "Id",
    
    CASE 
        WHEN c.full_name IS NULL OR TRIM(c.full_name) = '' THEN NULL
        ELSE SPLIT_PART(TRIM(c.full_name), ' ', 1)
    END AS "FirstName",
    
    CASE 
        WHEN c.full_name IS NULL OR TRIM(c.full_name) = '' THEN 'Unknown'
        WHEN POSITION(' ' IN TRIM(c.full_name)) = 0 THEN TRIM(c.full_name)
        ELSE SUBSTRING(TRIM(c.full_name) FROM POSITION(' ' IN TRIM(c.full_name)) + 1)
    END AS "LastName",
    
    TRIM(c.email) AS "Email",
    
    NULL::TEXT AS "Phone",
    
    NULL::TEXT AS "Title",
    
    NULL::TEXT AS "Role__c",
    
    NULL::TEXT AS "Preferred_Language__c",
    
    a.id AS "AccountId",
    
    c.id AS "Legacy_Contact_ID__c",
    
    NULL::TEXT AS "CreatedDate",
    
    NULL::TEXT AS "LastModifiedDate",
    
    0 AS "IsDeleted"

FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON c.account_ref = a.id