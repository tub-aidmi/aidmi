{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, ap.ap_id) AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN ap.funktion IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor') THEN ap.funktion
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN ap.sprache IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN ap.sprache
        ELSE NULL
    END AS "Preferred_Language__c",
    k.kunden_nr AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON ap.kunde = k.kunden_nr
