{{ config(materialized='table') }}

SELECT
    CAST(ap.ap_id AS TEXT) AS "Id",
    ap.vorname AS "FirstName",
    ap.nachname AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    ap.telefonnummer AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE ap.funktion
        WHEN 'Decision Maker' THEN 'Decision Maker'
        WHEN 'End User' THEN 'End User'
        WHEN 'Executive Sponsor' THEN 'Executive Sponsor'
        WHEN 'Technical Contact' THEN 'Technical Contact'
        ELSE NULL
    END AS "Role__c",
    CASE ap.sprache
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} ap
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} k
    ON TRIM(ap.kunde) = TRIM(k.kunden_nr)