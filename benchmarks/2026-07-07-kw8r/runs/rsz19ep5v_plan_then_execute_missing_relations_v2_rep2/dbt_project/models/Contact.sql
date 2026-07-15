{{ config(materialized='table') }}

SELECT 
    TRIM(UPPER(c.id)) AS "Id",
    INITCAP(TRIM(SPLIT_PART(c.full_name, ' ', 1))) AS "FirstName",
    COALESCE(INITCAP(TRIM(REGEXP_REPLACE(c.full_name, '^.* ([^ ]+)$', '\1'))), 'Unknown') AS "LastName",
    LOWER(TRIM(c.email))::TEXT AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    acc.id::TEXT AS "AccountId",
    c.id::TEXT AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON TRIM(UPPER(c.account_ref)) = TRIM(UPPER(acc.id))