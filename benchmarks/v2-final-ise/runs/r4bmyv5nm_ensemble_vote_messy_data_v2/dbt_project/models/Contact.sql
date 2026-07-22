{{ config(materialized='table') }}

SELECT
    src.id,
    INITCAP(TRIM(src.firstname)) AS "FirstName",
    CASE WHEN TRIM(src.lastname) = '' OR TRIM(src.lastname) IS NULL THEN 'Unknown' ELSE INITCAP(TRIM(src.lastname)) END AS "LastName",
    LOWER(TRIM(src.email)) AS "Email",
    TRIM(src.phone) AS "Phone",
    INITCAP(TRIM(src.title)) AS "Title",
    CASE UPPER(TRIM(COALESCE(src.role__c, '')))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(COALESCE(src.preferred_language__c, '')))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    CAST(src.accountid AS TEXT) AS "AccountId",
    src.id AS "Legacy_Contact_ID__c",
    CURRENT_DATE::TEXT AS "CreatedDate",
    CURRENT_DATE::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_messy_data_v2_src', 'contact') }} src