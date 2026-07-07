-- depends_on: {{ ref('Account') }}
{{ config(materialized='table') }}

SELECT
    MD5(ap.ap_id) AS "Id",
    INITCAP(TRIM(ap.vorname)) AS "FirstName",
    COALESCE(INITCAP(TRIM(ap.nachname)), 'Unknown') AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN LOWER(TRIM(ap.funktion)) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(TRIM(ap.funktion)) = 'end user' THEN 'End User'
        WHEN LOWER(TRIM(ap.funktion)) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(TRIM(ap.funktion)) = 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(TRIM(ap.sprache)) IN ('DE', 'EN', 'FR', 'ES', 'IT') THEN UPPER(TRIM(ap.sprache))
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