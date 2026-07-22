{{ config(materialized='table') }}

SELECT
    TRIM(CAST(id AS TEXT)) AS "Id",
    INITCAP(TRIM(firstname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(lastname)), 'Unknown') AS "LastName",
    NULLIF(NULLIF(LOWER(TRIM(email)), 'N/A'), '') AS "Email",
    TRIM(phone) AS "Phone",
    INITCAP(TRIM(title)) AS "Title",
    CASE LOWER(TRIM(COALESCE(role__c, '')))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'techniker' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        WHEN 'sponsor' THEN 'Executive Sponsor'
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'endanwender' THEN 'End User'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(COALESCE(preferred_language__c, '')))
        WHEN 'DE' THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(CAST(accountid AS TEXT)) AS "AccountId",
    TRIM(CAST(id AS TEXT)) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }}