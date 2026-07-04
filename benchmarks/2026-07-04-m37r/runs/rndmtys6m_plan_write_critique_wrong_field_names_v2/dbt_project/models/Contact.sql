{{ config(materialized='table') }}

SELECT
    MD5(ap.ap_id) AS "Id",
    INITCAP(TRIM(COALESCE(ap.vorname, ''))) AS "FirstName",
    INITCAP(TRIM(COALESCE(ap.nachname, 'Unknown Contact'))) AS "LastName",
    LOWER(TRIM(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    INITCAP(TRIM(ap.position)) AS "Title",
    CASE
        WHEN LOWER(ap.funktion) = 'decision maker' THEN 'Decision Maker'
        WHEN LOWER(ap.funktion) = 'end user' THEN 'End User'
        WHEN LOWER(ap.funktion) = 'technical contact' THEN 'Technical Contact'
        WHEN LOWER(ap.funktion) = 'executive sponsor' THEN 'Executive Sponsor'
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
    MD5(k.kunden_nr) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
ON
    ap.kunde = k.kunden_nr
