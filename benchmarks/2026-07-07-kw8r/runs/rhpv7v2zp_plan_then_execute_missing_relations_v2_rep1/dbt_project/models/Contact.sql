{{ config(materialized='table') }}

SELECT 
    UPPER(TRIM("id")) AS "Id",
    NULLIF(TRIM(SPLIT_PART("full_name", ' ', 1)), '') AS "FirstName",
    CASE 
        WHEN POSITION(' ' IN "full_name") > 0 THEN NULLIF(TRIM(SPLIT_PART("full_name", ' ', -1)), '') 
        ELSE 'UNKNOWN' 
    END AS "LastName",
    LOWER(TRIM("email")) AS "Email",
    NULL::TEXT AS "Phone",
    INITCAP(NULLIF(TRIM("company_name"), '')) AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    UPPER(TRIM("account_ref")) AS "AccountId",
    TRIM("id") AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }}