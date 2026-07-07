-- depends_on: {{ ref('Account') }}

{{ config(materialized='table') }}

SELECT
    ap.ap_id AS "Id",
    ap.vorname AS "FirstName",
    COALESCE(ap.nachname, 'Unknown') AS "LastName",
    ap.email_adresse AS "Email",
    ap.telefonnummer AS "Phone",
    ap.position AS "Title",
    CASE
        WHEN LOWER(ap.funktion) LIKE '%decision maker%' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) LIKE '%entscheider%' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) LIKE '%end user%' THEN 'End User'
        WHEN LOWER(ap.funktion) LIKE '%endbenutzer%' THEN 'End User'
        WHEN LOWER(ap.funktion) LIKE '%technical contact%' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) LIKE '%technisch%' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) LIKE '%executive sponsor%' THEN 'Executive Sponsor'
        WHEN LOWER(ap.funktion) LIKE '%geschaeftsfuehrer%' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(ap.sprache) = 'deutsch' THEN 'DE'
        WHEN LOWER(ap.sprache) = 'german' THEN 'DE'
        WHEN LOWER(ap.sprache) = 'englisch' THEN 'EN'
        WHEN LOWER(ap.sprache) = 'english' THEN 'EN'
        WHEN LOWER(ap.sprache) = 'franzoesisch' THEN 'FR'
        WHEN LOWER(ap.sprache) = 'french' THEN 'FR'
        WHEN LOWER(ap.sprache) = 'spanisch' THEN 'ES'
        WHEN LOWER(ap.sprache) = 'spanish' THEN 'ES'
        WHEN LOWER(ap.sprache) = 'italienisch' THEN 'IT'
        WHEN LOWER(ap.sprache) = 'italian' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    acc."Id" AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    NULL AS "CreatedDate",
    NULL AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ ref('Account') }} AS acc
ON
    ap.kunde = acc."Legacy_Customer_ID__c"