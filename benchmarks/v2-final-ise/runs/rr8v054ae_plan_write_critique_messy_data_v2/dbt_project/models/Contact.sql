{{ config(materialized='table') }}

SELECT 
    c.id AS "Id",
    c.firstname AS "FirstName",
    COALESCE(NULLIF(TRIM(c.lastname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(c.email), 'N/A') AS "Email",
    c.phone AS "Phone",
    c.title AS "Title",
    CASE 
        WHEN TRIM(LOWER(c.role__c)) IN ('decision maker', 'entscheider', 'end user', 'endanwender', 'technical contact', 'techniker', 'technischer ansprechpartner', 'executive sponsor', 'sponsor') THEN 
            CASE 
                WHEN TRIM(LOWER(c.role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
                WHEN TRIM(LOWER(c.role__c)) IN ('end user', 'endanwender') THEN 'End User'
                WHEN TRIM(LOWER(c.role__c)) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
                WHEN TRIM(LOWER(c.role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
            END
        WHEN c.role__c IS NULL OR TRIM(c.role__c) = '' THEN NULL
        ELSE NULL
    END AS "Role__c",
    CASE 
        WHEN TRIM(LOWER(c.preferred_language__c)) IN ('de', 'deutsch', 'german', 'en', 'englisch', 'english', 'fr', 'französisch', 'french') THEN 
            CASE 
                WHEN TRIM(LOWER(c.preferred_language__c)) IN ('de', 'deutsch', 'german') THEN 'DE'
                WHEN TRIM(LOWER(c.preferred_language__c)) IN ('en', 'englisch', 'english') THEN 'EN'
                WHEN TRIM(LOWER(c.preferred_language__c)) IN ('fr', 'französisch', 'french') THEN 'FR'
            END
        WHEN c.preferred_language__c IS NULL OR TRIM(c.preferred_language__c) = '' THEN NULL
        ELSE NULL
    END AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a ON c.accountid = a.id