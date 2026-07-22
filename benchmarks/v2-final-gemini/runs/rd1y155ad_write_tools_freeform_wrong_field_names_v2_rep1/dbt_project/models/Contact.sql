{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, 'N/A') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN LOWER(ap.funktion) LIKE '%decision maker%' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) LIKE '%end user%' THEN 'End User'
        WHEN LOWER(ap.funktion) LIKE '%technical contact%' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) LIKE '%executive sponsor%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(ap.sprache) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(ap.sprache) IN ('en', 'english') THEN 'EN'
        WHEN LOWER(ap.sprache) IN ('fr', 'french') THEN 'FR'
        WHEN LOWER(ap.sprache) IN ('es', 'spanish') THEN 'ES'
        WHEN LOWER(ap.sprache) IN ('it', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ap.kunde AS "AccountId", -- AccountId is kunden_nr from the kunden table
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
WHERE
    ap.ap_id IS NOT NULL
    AND COALESCE(ap.nachname, '') != ''
