{{ config(materialized='table') }}

SELECT 
    c.id AS "Id",
    SPLIT_PART(c.full_name, ' ', 1) AS "FirstName",
    CASE 
        WHEN POSITION(' ' IN c.full_name) > 0 
        THEN TRIM(SUBSTRING(c.full_name FROM POSITION(' ' IN c.full_name) + 1))
        ELSE COALESCE(TRIM(c.full_name), 'Unknown')
    END AS "LastName",
    c.email AS "Email",
    NULL::TEXT AS "Phone",
    NULL::TEXT AS "Title",
    -- Role__c enum: (Decision Maker, End User, Technical Contact, Executive Sponsor)
    -- Source provides no role information; consistent fallback is NULL.
    NULL::TEXT AS "Role__c",
    -- Preferred_Language__c enum: (DE, EN, FR, ES, IT)
    -- Source provides no language information; consistent fallback is NULL.
    NULL::TEXT AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_missing_relations_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_missing_relations_v2_src', 'account') }} a 
    ON c.account_ref = a.id