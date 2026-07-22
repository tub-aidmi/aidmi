{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    SPLIT_PART(c.full_name, ' ', 1) AS "FirstName",
    CASE 
        WHEN POSITION(' ' IN COALESCE(c.full_name, '')) > 0 
        THEN INITCAP(TRIM(SUBSTR(c.full_name, POSITION(' ' IN c.full_name) + 1)))
        ELSE INITCAP(COALESCE(c.full_name, 'Unknown'))
    END AS "LastName",
    c.email AS "Email",
    NULL::text AS "Phone",
    NULL::text AS "Title",
    NULL::text AS "Role__c",
    NULL::text AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON TRIM(LOWER(c.account_ref)) = TRIM(LOWER(a.id))