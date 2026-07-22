{{ config(materialized='table') }}

SELECT
    TRIM(id) AS "Id",
    TRIM(firstname) AS "FirstName",
    COALESCE(TRIM(lastname), TRIM(id)) AS "LastName",
    LOWER(TRIM(email)) AS "Email",
    TRIM(phone) AS "Phone",
    TRIM(title) AS "Title",
    CASE
        WHEN LOWER(TRIM(role__c)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(role__c)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(role__c)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(role__c)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(preferred_language__c)) = 'DE' THEN 'DE'
        WHEN UPPER(TRIM(preferred_language__c)) = 'EN' THEN 'EN'
        WHEN UPPER(TRIM(preferred_language__c)) = 'FR' THEN 'FR'
        WHEN UPPER(TRIM(preferred_language__c)) = 'ES' THEN 'ES'
        WHEN UPPER(TRIM(preferred_language__c)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(accountid) AS "AccountId",
    TRIM(id) AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }}
