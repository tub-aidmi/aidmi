{{ config(materialized='table') }}

WITH contacts_with_accounts AS (
    SELECT
        kontakte.kontakt_id,
        kontakte.rufname,
        kontakte.familienname,
        kontakte.kontakt_email,
        kontakte.tel,
        kontakte.berufsbezeichnung,
        kontakte.rolle,
        kontakte.korrespondenzsprache,
        kontakte.kd_nummer,
        kunden.kundennummer AS account_legacy_id
    FROM
        {{ source('fixture_master_v2_src', 'master_kontakte') }} AS kontakte
    LEFT JOIN
        {{ source('fixture_master_v2_src', 'master_kunden') }} AS kunden
        ON kontakte.kd_nummer = kunden.kundennummer
)

SELECT
    CAST(gen_random_uuid() AS TEXT) AS "Id",
    TRIM(INITCAP(contacts.rufname)) AS "FirstName",
    COALESCE(TRIM(INITCAP(contacts.familienname)), 'Unknown') AS "LastName",
    TRIM(LOWER(contacts.kontakt_email)) AS "Email",
    TRIM(contacts.tel) AS "Phone",
    TRIM(INITCAP(contacts.berufsbezeichnung)) AS "Title",
    CASE LOWER(TRIM(contacts.rolle))
        WHEN 'entscheider' THEN 'Decision Maker'
        WHEN 'endnutzer' THEN 'End User'
        WHEN 'technischer_kontakt' THEN 'Technical Contact'
        WHEN 'executive_sponsor' THEN 'Executive Sponsor'
        ELSE NULL
    END AS "Role__c",
    CASE UPPER(TRIM(contacts.korrespondenzsprache))
        WHEN 'DE' THEN 'DE'
        WHEN 'EN' THEN 'EN'
        WHEN 'FR' THEN 'FR'
        WHEN 'ES' THEN 'ES'
        WHEN 'IT' THEN 'IT'
        ELSE NULL
    END AS "Preferred_Language__c",
    -- Generate a Salesforce-style Account ID by hashing the legacy customer number
    MD5(contacts.account_legacy_id) AS "AccountId",
    TRIM(contacts.kontakt_id) AS "Legacy_Contact_ID__c",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "CreatedDate",
    TO_CHAR(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.MSZ') AS "LastModifiedDate",
    0 AS "IsDeleted"
FROM
    contacts_with_accounts AS contacts