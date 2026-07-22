{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    COALESCE(NULLIF(TRIM(INITCAP(c.firstname)), ''), 'Unknown') AS "FirstName",
    COALESCE(NULLIF(TRIM(INITCAP(c.lastname)), ''), 'N/A') AS "LastName",
    TRIM(LOWER(c.email)) AS "Email",
    TRIM(c.phone) AS "Phone",
    TRIM(INITCAP(c.title)) AS "Title",
    CASE
        WHEN LOWER(TRIM(c.role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(c.role__c)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(c.role__c)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(c.role__c)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(SUBSTRING(c.preferred_language__c, 1, 2))) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(SUBSTRING(c.preferred_language__c, 1, 2)))
        ELSE NULL
    END AS "Preferred_Language__c",
    a.id AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    '2024-01-01' AS "CreatedDate",
    '2024-01-01' AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
LEFT JOIN {{ source('fixture_messy_data_v2_src', 'account') }} a
    ON c.accountid = a.id