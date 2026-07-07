{{ config(materialized='table') }}

SELECT
    TRIM(ap.ap_id) AS "Id",
    TRIM(ap.vorname) AS "FirstName",
    COALESCE(TRIM(ap.nachname), 'Unknown') AS "LastName",
    TRIM(ap.email_adresse) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE
        WHEN LOWER(ap.funktion) LIKE '%entscheider%' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) LIKE '%anwender%' THEN 'End User'
        WHEN LOWER(ap.funktion) LIKE '%technisch%' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) LIKE '%führungskraft%' OR LOWER(ap.funktion) LIKE '%geschäftsführer%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(ap.sprache) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(ap.sprache) IN ('en', 'englisch', 'english') THEN 'EN'
        WHEN LOWER(ap.sprache) IN ('fr', 'französisch', 'french') THEN 'FR'
        WHEN LOWER(ap.sprache) IN ('es', 'spanisch', 'spanish') THEN 'ES'
        WHEN LOWER(ap.sprache) IN ('it', 'italienisch', 'italian') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(TRIM(k.kunden_nr)) AS "AccountId",
    TRIM(ap.ap_id) AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    TRIM(ap.kunde) = TRIM(k.kunden_nr)