-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }}
-- depends_on: {{ source('fixture_wrong_field_names_v2_src', 'kunden') }}

{{ config(materialized='table') }}

SELECT
    MD5(ap.ap_id) AS "Id",
    TRIM(INITCAP(ap.vorname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(ap.nachname)), 'Unknown Contact ' || ap.ap_id) AS "LastName",
    TRIM(LOWER(ap.email_adresse)) AS "Email",
    TRIM(ap.telefonnummer) AS "Phone",
    TRIM(ap.position) AS "Title",
    CASE TRIM(LOWER(ap.funktion))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE TRIM(UPPER(ap.sprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(k.kunden_nr) AS "AccountId",
    ap.ap_id AS "Legacy_Contact_ID__c",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "CreatedDate",
    TO_CHAR(CURRENT_TIMESTAMP, 'YYYY-MM-DD HH24:MI:SS') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_wrong_field_names_v2_src', 'ansprechpartner') }} AS ap
LEFT JOIN
    {{ source('fixture_wrong_field_names_v2_src', 'kunden') }} AS k
    ON ap.kunde = k.kunden_nr