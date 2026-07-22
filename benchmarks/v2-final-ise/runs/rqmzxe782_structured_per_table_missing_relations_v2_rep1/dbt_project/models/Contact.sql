{{ config(materialized='table') }}

SELECT 
    sc.id AS "Id",
    TRIM(SPLIT_PART(sc.full_name, ' ', 1)) AS "FirstName",
    COALESCE(
        CASE 
            WHEN POSITION(' ' IN sc.full_name) > 0 THEN TRIM(SUBSTRING(sc.full_name FROM POSITION(' ' IN sc.full_name) + 1))
            ELSE sc.full_name 
        END,
        'Unknown'
    ) AS "LastName",
    LOWER(TRIM(sc.email)) AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    acc.id AS "AccountId",
    sc.id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0::INTEGER AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} sc
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} acc 
    ON TRIM(sc.account_ref) = TRIM(acc.id)