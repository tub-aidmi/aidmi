{{ config(materialized='table') }}

SELECT
    kontakt_id AS "Id",
    rufname AS "FirstName",
    COALESCE(TRIM(familienname), 'N/A') AS "LastName",
    kontakt_email AS "Email",
    tel AS "Phone",
    berufsbezeichnung AS "Title",
    CASE LOWER(TRIM(rolle))
        WHEN 'decision maker' THEN 'Decision Maker'
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'end user' THEN 'End User'
        WHEN 'endanwender' THEN 'End User'
        WHEN 'technical contact' THEN 'Technical Contact'
        WHEN 'technischer ansprechpartner' THEN 'Technical Contact'
        WHEN 'techniker' THEN 'Technical Contact'
        WHEN 'executive sponsor' THEN 'Executive Sponsor'
        WHEN 'sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE LOWER(TRIM(korrespondenzsprache))
        WHEN 'english' THEN 'EN'
        WHEN 'englisch' THEN 'EN'
        WHEN 'en' THEN 'EN'
        WHEN 'deutsch' THEN 'DE'
        WHEN 'de' THEN 'DE'
        WHEN 'german' THEN 'DE'
        WHEN 'french' THEN 'FR'
        WHEN 'französisch' THEN 'FR'
        WHEN 'fr' THEN 'FR'
        ELSE NULL
    END AS "Preferred_Language__c",
    kd_nummer AS "AccountId",
    kontakt_id AS "Legacy_Contact_ID__c",
    NULL::text AS "CreatedDate",
    NULL::text AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source(source_name, source_table) }}
