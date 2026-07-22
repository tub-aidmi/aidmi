{{ config(materialized='table') }}

WITH contact_source AS (
    SELECT
        ap_id,
        TRIM(vorname) AS first_name,
        TRIM(nachname) AS last_name,
        TRIM(email_adresse) AS email,
        TRIM(telefonnummer) AS phone,
        TRIM(position) AS title,
        TRIM(funktion) AS role_source,
        TRIM(sprache) AS language_source,
        TRIM(kunde) AS account_key
    FROM {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
)

SELECT
    ap_id AS "Id",
    INITCAP(first_name) AS "FirstName",
    INITCAP(last_name) AS "LastName",
    email AS "Email",
    phone AS "Phone",
    title AS "Title",
    CASE
        WHEN UPPER(role_source) LIKE '%ENTSCHEIDER%' THEN 'Decision Maker'
        WHEN UPPER(role_source) LIKE '%ENDNUTZER%' THEN 'End User'
        WHEN UPPER(role_source) LIKE '%TECHNIK%' OR UPPER(role_source) LIKE '%TECHNISCH%' THEN 'Technical Contact'
        WHEN UPPER(role_source) LIKE '%GESCHÄFTSFÜHRER%' OR UPPER(role_source) LIKE '%EXECUTIVE%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN UPPER(language_source) LIKE '%DEUTSCH%' THEN 'DE'
        WHEN UPPER(language_source) LIKE '%ENGLISCH%' THEN 'EN'
        WHEN UPPER(language_source) LIKE '%FRANZÖSISCH%' THEN 'FR'
        WHEN UPPER(language_source) LIKE '%SPANISCH%' THEN 'ES'
        WHEN UPPER(language_source) LIKE '%ITALIENISCH%' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    kunden.kunden_nr AS "AccountId",
    ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM contact_source
LEFT JOIN {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS kunden
    ON contact_source.account_key = kunden.kunden_nr