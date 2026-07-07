{{ config(materialized='table') }}

SELECT
    MD5(ap.ap_id) AS "Id",
    ap.vorname AS "FirstName",
    ap.nachname AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN TRIM(ap.funktion) IN ('Decision Maker', 'End User', 'Technical Contact', 'Executive Sponsor')
        THEN TRIM(ap.funktion)
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN TRIM(ap.sprache) IN ('DE', 'EN', 'FR', 'ES', 'IT')
        THEN TRIM(ap.sprache)
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
ON
    ap.kunde = k.kunden_nr
