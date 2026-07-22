{{ config(materialized='table') }}

SELECT
    MD5(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), 'Unknown') AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN LOWER(ap.funktion) LIKE '%entscheider%' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) LIKE '%endnutzer%' THEN 'End User'
        WHEN LOWER(ap.funktion) LIKE '%technisch%' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) LIKE '%vorstand%' OR LOWER(ap.funktion) LIKE '%geschäftsführer%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(ap.sprache) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(ap.sprache)
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(k.kunden_nr) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON ap.kunde = k.kunden_nr
