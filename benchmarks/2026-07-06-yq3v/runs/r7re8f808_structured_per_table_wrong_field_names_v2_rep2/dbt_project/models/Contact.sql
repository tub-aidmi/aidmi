{{ config(materialized='table') }}

SELECT
    MD5(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    TRIM(ap.nachname) AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN LOWER(ap.funktion) LIKE '%entscheider%' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) LIKE '%anwender%' THEN 'End User'
        WHEN LOWER(ap.funktion) LIKE '%techniker%' OR LOWER(ap.funktion) LIKE '%it%' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) LIKE '%geschäftsführer%' OR LOWER(ap.funktion) LIKE '%leiter%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(ap.sprache) IN ('deutsch', 'de') THEN 'DE'
        WHEN LOWER(ap.sprache) IN ('englisch', 'en') THEN 'EN'
        WHEN LOWER(ap.sprache) IN ('französisch', 'fr') THEN 'FR'
        WHEN LOWER(ap.sprache) IN ('spanisch', 'es') THEN 'ES'
        WHEN LOWER(ap.sprache) IN ('italienisch', 'it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(k.kunden_nr) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    ap.kunde = k.kunden_nr