-- dbt model for the Contact target table
{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    TRIM(INITCAP(c.firstname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(c.lastname)), 'Unknown') AS "LastName",
    TRIM(LOWER(c.email)) AS "Email",
    TRIM(c.phone) AS "Phone",
    TRIM(c.title) AS "Title",
    CASE
        WHEN LOWER(TRIM(c.role__c)) IN ('decision maker', 'entscheider') THEN 'Decision Maker'
        WHEN LOWER(TRIM(c.role__c)) IN ('end user', 'endanwender') THEN 'End User'
        WHEN LOWER(TRIM(c.role__c)) IN ('technical contact', 'techniker', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(c.role__c)) IN ('executive sponsor', 'sponsor') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(REPLACE(c.preferred_language__c, 'ä', 'ae')))
        WHEN 'DE' THEN 'DE'
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'GERMAN' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        WHEN 'FRANZOESISCH' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS c