{{ config(materialized='table') }}

SELECT
    contact.id AS "Id",
    NULLIF(TRIM(contact.firstname), '') AS "FirstName",
    COALESCE(NULLIF(TRIM(contact.lastname), ''), 'Unknown') AS "LastName",
    NULLIF(TRIM(contact.email), '') AS "Email",
    NULLIF(TRIM(contact.phone), '') AS "Phone",
    NULLIF(TRIM(contact.title), '') AS "Title",
    CASE UPPER(TRIM(contact.role__c))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'ENDANWENDER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN 'TECHNIKER' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN 'SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(contact.preferred_language__c))
        WHEN 'DEUTSCH' THEN 'DE'
        WHEN 'DE' THEN 'DE'
        WHEN 'GERMAN' THEN 'DE'
        WHEN 'ENGLISCH' THEN 'EN'
        WHEN 'EN' THEN 'EN'
        WHEN 'ENGLISH' THEN 'EN'
        WHEN 'FRANZÖSISCH' THEN 'FR'
        WHEN 'FR' THEN 'FR'
        WHEN 'FRENCH' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    contact.accountid AS "AccountId",
    contact.id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_messy_data_v2_src', 'contact') }} AS contact
