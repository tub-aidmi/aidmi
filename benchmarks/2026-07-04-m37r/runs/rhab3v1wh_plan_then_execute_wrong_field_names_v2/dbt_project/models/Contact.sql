{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    TRIM(INITCAP(ap.vorname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(ap.nachname)), 'Unknown Contact') AS "LastName",
    TRIM(LOWER(ap.email_adresse)) AS "Email",
    ap.telefonnummer AS "Phone",
    TRIM(INITCAP(ap.position)) AS "Title",
    CASE
        WHEN LOWER(ap.funktion) LIKE '%decision maker%' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) LIKE '%end user%' THEN 'End User'
        WHEN LOWER(ap.funktion) LIKE '%technical contact%' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) LIKE '%executive sponsor%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(ap.sprache) = 'DE' THEN 'DE'
        WHEN UPPER(ap.sprache) = 'EN' THEN 'EN'
        WHEN UPPER(ap.sprache) = 'FR' THEN 'FR'
        WHEN UPPER(ap.sprache) = 'ES' THEN 'ES'
        WHEN UPPER(ap.sprache) = 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    ap.kunde AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    CURRENT_TIMESTAMP AS "CreatedDate",
    CURRENT_TIMESTAMP AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
