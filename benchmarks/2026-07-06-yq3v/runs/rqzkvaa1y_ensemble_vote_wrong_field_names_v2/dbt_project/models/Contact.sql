{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, 'Unknown') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN LOWER(ap.funktion) LIKE '%decision%' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) LIKE '%end user%' THEN 'End User'
        WHEN LOWER(ap.funktion) LIKE '%technical%' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) LIKE '%executive%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(ap.sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    ap.kunde AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap