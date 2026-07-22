{{ config(materialized='table') }}

SELECT
    CAST(ap.ap_id AS TEXT) AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    TRIM(ap.nachname) AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE
        WHEN INITCAP(TRIM(ap.funktion)) = 'Decision Maker' THEN 'Decision Maker'
        WHEN INITCAP(TRIM(ap.funktion)) = 'End User' THEN 'End User'
        WHEN INITCAP(TRIM(ap.funktion)) = 'Technical Contact' THEN 'Technical Contact'
        WHEN INITCAP(TRIM(ap.funktion)) = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(ap.sprache))
        ELSE NULL
    END AS "Preferred_Language__c",
    '001' || LPAD(CAST(SUBSTRING(k.kunden_nr FROM '\d+') AS INTEGER)::TEXT, 12, '0') AS "AccountId",
    CAST(ap.ap_id AS TEXT) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON ap.kunde = k.kunden_nr