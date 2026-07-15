{{ config(materialized='table') }}

WITH contact_data AS (
    SELECT 
        c.id,
        c.full_name,
        c.email,
        c.account_ref,
        c.company_name,
        a.id AS account_id
    FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
    LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a ON 
        c.account_ref = a.id OR c.company_name = a.name
)

SELECT
    id AS "Id",
    CASE 
        WHEN full_name ~ ' ' THEN 
            SPLIT_PART(full_name, ' ', 1) 
        ELSE 
            NULL 
    END AS "FirstName",
    CASE 
        WHEN full_name ~ ' ' THEN 
            SUBSTRING(full_name FROM POSITION(' ' IN full_name) + 1) 
        ELSE 
            full_name 
    END AS "LastName",
    email AS "Email",
    NULL AS "Phone",
    NULL AS "Title",
    NULL AS "Role__c",
    NULL AS "Preferred_Language__c",
    account_id AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_data
