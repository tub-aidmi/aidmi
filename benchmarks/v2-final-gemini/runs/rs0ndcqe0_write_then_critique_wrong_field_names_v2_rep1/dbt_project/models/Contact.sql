{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, 'UNKNOWN') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN ap.funktion = 'Decision Maker' THEN 'Decision Maker'
        WHEN ap.funktion = 'End User' THEN 'End User'
        WHEN ap.funktion = 'Technical Contact' THEN 'Technical Contact'
        WHEN ap.funktion = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN ap.sprache = 'DE' THEN 'DE'
        WHEN ap.sprache = 'EN' THEN 'EN'
        WHEN ap.sprache = 'FR' THEN 'FR'
        WHEN ap.sprache = 'ES' THEN 'ES'
        WHEN ap.sprache = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ap.kunde AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
```