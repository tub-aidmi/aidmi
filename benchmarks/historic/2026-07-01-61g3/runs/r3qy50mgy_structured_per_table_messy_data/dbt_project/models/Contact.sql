{{ config(materialized='table') }}

SELECT
    Id,
    INITCAP(TRIM(FirstName)) AS FirstName,
    CASE WHEN TRIM(COALESCE(LastName, '')) = '' THEN 'Unknown' ELSE INITCAP(TRIM(LastName)) END AS LastName,
    LOWER(TRIM(Email)) AS Email,
    Phone,
    INITCAP(TRIM(Title)) AS Title,
    CASE
        WHEN LOWER(TRIM(COALESCE(Role__c, ''))) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(COALESCE(Role__c, ''))) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(COALESCE(Role__c, ''))) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(COALESCE(Role__c, ''))) = 'entscheider' THEN 'Decision Maker'
        ELSE NULL
    END AS Role__c,
    CASE
        WHEN LOWER(TRIM(COALESCE(Preferred_Language__c, ''))) IN ('de', 'deutsch') THEN 'DE'
        WHEN LOWER(TRIM(COALESCE(Preferred_Language__c, ''))) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(COALESCE(Preferred_Language__c, ''))) = 'fr' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    AccountId,
    NULL AS "Legacy_Contact_ID__c",
    NULL AS CreatedDate,
    NULL AS LastModifiedDate,
    0 AS IsDeleted
FROM {{ source('fixture_messy_data_src', 'Contact') }}