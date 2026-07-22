{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, 'Unknown') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN ap.funktion ILIKE 'Decision Maker' THEN 'Decision Maker'
        WHEN ap.funktion ILIKE 'End User' THEN 'End User'
        WHEN ap.funktion ILIKE 'Technical Contact' THEN 'Technical Contact'
        WHEN ap.funktion ILIKE 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN ap.sprache ILIKE 'DE' THEN 'DE'
        WHEN ap.sprache ILIKE 'EN' THEN 'EN'
        WHEN ap.sprache ILIKE 'FR' THEN 'FR'
        WHEN ap.sprache ILIKE 'ES' THEN 'ES'
        WHEN ap.sprache ILIKE 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ap.kunde AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
