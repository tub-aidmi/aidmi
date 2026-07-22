{{ config(materialized='table') }}

SELECT
    CAST(c.id AS TEXT) AS "Id",
    CASE 
        WHEN POSITION(' ' IN REVERSE(c.full_name)) > 0 THEN 
            LEFT(c.full_name, LENGTH(c.full_name) - POSITION(' ' IN REVERSE(c.full_name)))
        ELSE c.full_name
    END AS "FirstName",
    CASE 
        WHEN POSITION(' ' IN REVERSE(c.full_name)) > 0 THEN 
            TRIM(SUBSTRING(REVERSE(c.full_name) FROM 1 FOR POSITION(' ' IN REVERSE(c.full_name)) - 1))
        ELSE ''
    END AS "LastName",
    CAST(c.email AS TEXT) AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    CAST(c.account_ref AS TEXT) AS "AccountId",
    CAST(c.id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c