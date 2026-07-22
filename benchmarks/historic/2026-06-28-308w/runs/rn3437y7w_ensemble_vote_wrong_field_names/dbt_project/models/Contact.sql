
{{ config(materialized='table') }}

SELECT
    a.ap_id AS "Id",
    TRIM(a.vorname) AS "FirstName",
    COALESCE(TRIM(a.nachname), 'Unknown') AS "LastName",
    LOWER(TRIM(a.email_adresse)) AS "Email",
    TRIM(a.telefonnummer) AS "Phone",
    TRIM(a.position) AS "Title",
    CASE
        WHEN a.funktion = 'Decision Maker' THEN 'Decision Maker'
        WHEN a.funktion = 'End User' THEN 'End User'
        WHEN a.funktion = 'Technical Contact' THEN 'Technical Contact'
        WHEN a.funktion = 'Executive Sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN a.sprache = 'DE' THEN 'DE'
        WHEN a.sprache = 'EN' THEN 'EN'
        WHEN a.sprache = 'FR' THEN 'FR'
        WHEN a.sprache = 'ES' THEN 'ES'
        WHEN a.sprache = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    a.kunde AS "AccountId",
    a.ap_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0::integer AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_src', 'ansprechpartner') }} AS a
