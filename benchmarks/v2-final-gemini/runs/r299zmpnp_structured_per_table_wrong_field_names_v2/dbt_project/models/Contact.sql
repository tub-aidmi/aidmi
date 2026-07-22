-- no-op

{{ config(materialized='table') }}

SELECT
    MD5(ap_id) AS "Id",
    vorname AS "FirstName",
    COALESCE(nachname, 'Unknown') AS "LastName",
    email_adresse AS "Email",
    telefonnummer AS "Phone",
    position AS "Title",
    CASE
        WHEN TRIM(UPPER(funktion)) = 'DECISION MAKER' THEN 'Decision Maker'
        WHEN TRIM(UPPER(funktion)) = 'ENTSCHEIDER' THEN 'Decision Maker'
        WHEN TRIM(UPPER(funktion)) = 'END USER' THEN 'End User'
        WHEN TRIM(UPPER(funktion)) = 'ENDANWENDER' THEN 'End User'
        WHEN TRIM(UPPER(funktion)) = 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN TRIM(UPPER(funktion)) = 'TECHNISCHER ANSPRECHPARTNER' THEN 'Technical Contact'
        WHEN TRIM(UPPER(funktion)) = 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        WHEN TRIM(UPPER(funktion)) = 'GESCHÄFTSFÜHRER' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(UPPER(sprache)) = 'DE' THEN 'DE'
        WHEN TRIM(UPPER(sprache)) = 'EN' THEN 'EN'
        WHEN TRIM(UPPER(sprache)) = 'FR' THEN 'FR'
        WHEN TRIM(UPPER(sprache)) = 'ES' THEN 'ES'
        WHEN TRIM(UPPER(sprache)) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(kunde) AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}