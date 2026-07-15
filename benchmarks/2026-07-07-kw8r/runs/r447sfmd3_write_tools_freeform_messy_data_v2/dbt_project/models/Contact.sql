{{ config(materialized='table') }}

SELECT
    CAST(id AS TEXT) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    INITCAP(TRIM(lastname)) AS "LastName",
    LOWER(TRIM(COALESCE(email, ''))) AS "Email",
    phone AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE LOWER(TRIM(COALESCE(role__c, '')))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(COALESCE(preferred_language__c, '')))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    accountid AS "AccountId",
    id AS "Legacy_Contact_ID__c",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "CreatedDate",
    CAST(CURRENT_TIMESTAMP AS TEXT) AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}
