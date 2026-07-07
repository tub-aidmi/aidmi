-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolve
{{ config(materialized='table') }}

WITH contacts_joined_accounts AS (
    SELECT
        k.*,
        ku.kundennummer
    FROM
        {{ source('fixture_master_v2_src', 'master_kontakte') }} AS k
    LEFT JOIN
        {{ source('fixture_master_v2_src', 'master_kunden') }} AS ku
        ON k.kd_nummer = ku.kundennummer
)
SELECT
    MD5(k.kontakt_id) AS "Id",
    INITCAP(TRIM(k.rufname)) AS "FirstName",
    INITCAP(TRIM(COALESCE(k.familienname, k.kontakt_id))) AS "LastName",
    LOWER(TRIM(k.kontakt_email)) AS "Email",
    TRIM(k.tel) AS "Phone",
    INITCAP(TRIM(k.berufsbezeichnung)) AS "Title",
    CASE UPPER(TRIM(k.rolle))
        WHEN 'DECISION MAKER' THEN 'Decision Maker'
        WHEN 'END USER' THEN 'End User'
        WHEN 'TECHNICAL CONTACT' THEN 'Technical Contact'
        WHEN 'EXECUTIVE SPONSOR' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(k.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    MD5(k.kd_nummer) AS "AccountId",
    k.kontakt_id AS "Legacy_Contact_ID__c",
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    contacts_joined_accounts AS k