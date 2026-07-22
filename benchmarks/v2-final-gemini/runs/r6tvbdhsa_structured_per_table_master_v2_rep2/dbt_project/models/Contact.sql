-- noinspection SqlDialectInspection
-- noinspection SqlNoDataSourceInspection

{{ config(materialized='table') }}

SELECT
    TRIM(mk.kontakt_id) AS "Id",
    TRIM(mk.rufname) AS "FirstName",
    COALESCE(TRIM(mk.familienname), 'Unknown') AS "LastName",
    LOWER(TRIM(mk.kontakt_email)) AS "Email",
    TRIM(mk.tel) AS "Phone",
    TRIM(mk.berufsbezeichnung) AS "Title",
    CASE
        WHEN LOWER(TRIM(mk.rolle)) IN ('decision maker', 'entscheidungsträger') THEN 'Decision Maker'
        WHEN LOWER(TRIM(mk.rolle)) IN ('end user', 'endnutzer') THEN 'End User'
        WHEN LOWER(TRIM(mk.rolle)) IN ('technical contact', 'technischer ansprechpartner') THEN 'Technical Contact'
        WHEN LOWER(TRIM(mk.rolle)) IN ('executive sponsor', 'leitender angestellter', 'geschäftsführer') THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('de', 'deutsch', 'german') THEN 'DE'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('en', 'english', 'englisch') THEN 'EN'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('fr', 'french', 'französisch') THEN 'FR'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('es', 'spanish', 'spanisch') THEN 'ES'
        WHEN LOWER(TRIM(mk.korrespondenzsprache)) IN ('it', 'italian', 'italienisch') THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    TRIM(mk.kd_nummer) AS "AccountId",
    TRIM(mk.kontakt_id) AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    {{ source('fixture_master_v2_src', 'master_kontakte') }} AS mk