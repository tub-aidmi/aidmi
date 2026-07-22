{{ config(materialized='table') }}

SELECT
    CAST(c.id AS TEXT) AS "Id",
    CASE 
        WHEN c.full_name IS NOT NULL AND TRIM(c.full_name) LIKE '% %' THEN TRIM(SPLIT_PART(c.full_name, ' ', 1))
        ELSE ''
    END AS "FirstName",
    COALESCE(
        CASE 
            WHEN c.full_name IS NULL OR TRIM(c.full_name) = '' THEN 'Unknown'
            WHEN c.full_name NOT LIKE '% %' THEN TRIM(c.full_name)
            ELSE TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))
        END,
         'Unknown'
     ) AS "LastName",
    CAST(NULLIF(TRIM(c.email), '') AS TEXT) AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    NULL::TEXT AS "Role__c",
    NULL::TEXT AS "Preferred_Language__c",
    a.id AS "AccountId",
    CAST(c.id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON c.account_ref = a.id;