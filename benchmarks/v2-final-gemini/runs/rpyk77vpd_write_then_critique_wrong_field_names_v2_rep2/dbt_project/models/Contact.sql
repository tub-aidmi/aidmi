-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

WITH
ansprechpartner_cleaned AS (
    SELECT
        TRIM(ap.ap_id) AS ap_id,
        TRIM(ap.vorname) AS vorname,
        TRIM(ap.nachname) AS nachname,
        TRIM(ap.email_adresse) AS email_adresse,
        TRIM(ap.telefonnummer) AS telefonnummer,
        TRIM(ap.position) AS position,
        TRIM(ap.funktion) AS funktion,
        TRIM(ap.sprache) AS sprache,
        TRIM(ap.kunde) AS kunde,
        k.kunden_nr AS account_kunden_nr
    FROM
        {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
    LEFT JOIN
        {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
        ON TRIM(ap.kunde) = TRIM(k.kunden_nr)
)
SELECT
    ap_id AS "Id",
    INITCAP(vorname) AS "FirstName",
    COALESCE(INITCAP(nachname), 'Unknown') AS "LastName",
    LOWER(email_adresse) AS "Email",
    telefonnummer AS "Phone",
    INITCAP(position) AS "Title",
    CASE
        WHEN LOWER(funktion) = 'entscheider' THEN 'Decision Maker'
        WHEN LOWER(funktion) = 'endnutzer' THEN 'End User'
        WHEN LOWER(funktion) = 'technischer kontakt' THEN 'Technical Contact'
        WHEN LOWER(funktion) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(sprache) IN ('deutsch', 'de') THEN 'DE'
        WHEN LOWER(sprache) IN ('englisch', 'en') THEN 'EN'
        WHEN LOWER(sprache) IN ('français', 'fr') THEN 'FR'
        WHEN LOWER(sprache) IN ('español', 'es') THEN 'ES'
        WHEN LOWER(sprache) IN ('italiano', 'it') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    account_kunden_nr AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    ansprechpartner_cleaned