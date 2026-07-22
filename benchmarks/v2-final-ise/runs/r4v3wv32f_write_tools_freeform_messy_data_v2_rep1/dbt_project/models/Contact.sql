{{ config(materialized='table') }}

SELECT
    c.id AS "Id",
    NULLIF(TRIM(c.firstname), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(c.lastname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(c.email), '') AS "Email",
    NULLIF(TRIM(c.phone), '') AS "Phone",
    NULLIF(TRIM(c.title), '') AS "Title",
    CASE
        WHEN UPPER(NULLIF(TRIM(c.role__c), '')) IN ('DECISION MAKER', 'ENTSCHEIDER', 'ENTSCHEIDERIN') THEN 'Decision Maker'
        WHEN UPPER(NULLIF(TRIM(c.role__c), '')) IN ('END USER', 'ENDANWENDER', 'ENDANWENDERIN') THEN 'End User'
        WHEN UPPER(NULLIF(TRIM(c.role__c), '')) IN ('TECHNICAL CONTACT', 'TECHNISCHER ANSPRECHPARTNER', 'TECHNISCHE ANSPRECHPARTNERIN', 'TECHNIKER', 'TECHNIKERIN') THEN 'Technical Contact'
        WHEN UPPER(NULLIF(TRIM(c.role__c), '')) IN ('EXECUTIVE SPONSOR', 'SPONSOR', 'SPONSORIN') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(NULLIF(TRIM(c.preferred_language__c), '')) IN ('DE', 'DEUTSCH', 'GERMAN') THEN 'DE'
        WHEN UPPER(NULLIF(TRIM(c.preferred_language__c), '')) IN ('EN', 'ENGLISH', 'ENGLISCH') THEN 'EN'
        WHEN UPPER(NULLIF(TRIM(c.preferred_language__c), '')) IN ('FR', 'FRENCH', 'FRANZÖSISCH', 'FRANZOESISCH') THEN 'FR'
        WHEN UPPER(NULLIF(TRIM(c.preferred_language__c), '')) IN ('ES', 'SPANISH') THEN 'ES'
        WHEN UPPER(NULLIF(TRIM(c.preferred_language__c), '')) IN ('IT', 'ITALIAN') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    c.accountid AS "AccountId",
    c.id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP::TEXT AS "CreatedDate",
    CURRENT_TIMESTAMP::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"

FROM {{ source('fixture_messy_data_v2_src', 'contact') }} c
