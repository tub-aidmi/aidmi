{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, '') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE ap.funktion
        WHEN 'Decision Maker' THEN 'Decision Maker'
        WHEN 'End User' THEN 'End User'
        WHEN 'Technical Contact' THEN 'Technical Contact'
        WHEN 'Executive Sponsor' THEN 'Executive Sponsor'
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
    ap.kunde AS "AccountId", -- maps directly to kunden.kunden_nr, which becomes Account.Id
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
