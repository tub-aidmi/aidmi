{{ config(materialized='table') }}

SELECT
    -- Account identifier from source customer number
    m.kundennummer AS "Id",

    -- Company name; fallback for NULLs to satisfy NOT NULL constraint
    COALESCE(INITCAP(TRIM(m.unternehmensname)), 'Unknown Customer') AS "Name",

    -- ERP number preserved as-is (may be NULL)
    m.erp_nr AS "ERP_Number__c",

    -- Customer tier: normalize inconsistent casing and German variants ("Platin" → "Platinum", "Silber" → "Silver")
    CASE LOWER(TRIM(m.kundenklasse))
        WHEN 'gold'      THEN 'Gold'
        WHEN 'silver', 'silber' THEN 'Silver'
        WHEN 'bronze'   THEN 'Bronze'
        WHEN 'platinum', 'platin' THEN 'Platinum'
        ELSE NULL
    END AS "Customer_Tier__c",

    -- Sales region; treat empty strings and NULL as missing data
    CASE
        WHEN TRIM(m.vertriebsgebiet) = '' OR m.vertriebsgebiet IS NULL THEN NULL
        ELSE INITCAP(TRIM(m.vertriebsgebiet))
    END AS "Region__c",

    -- Industry: translate German industry terms to English
    CASE INITCAP(TRIM(m.industrie))
        WHEN 'Finanzen'         THEN 'Finance'
        WHEN 'Gesundheitswesen' THEN 'Healthcare'
        WHEN 'Industrie'        THEN 'Manufacturing'
        WHEN 'Technologie'      THEN 'Technology'
        ELSE INITCAP(TRIM(m.industrie))
    END AS "Industry",

    -- Website / homepage as-is
    m.homepage AS "Website",

    -- Billing city; normalize capitalization
    INITCAP(TRIM(m.stadt)) AS "BillingCity",

    -- Billing country: translate German country names to English (partial mapping with INITCAP fallback)
    CASE LOWER(TRIM(m.land_region))
        WHEN 'afghanistan'                    THEN 'Afghanistan'
        WHEN 'alandinseln'                    THEN 'Åland Islands'
        WHEN 'albanien'                       THEN 'Albania'
        WHEN 'algerien'                       THEN 'Algeria'
        WHEN 'amerikanisch-samoa'             THEN 'American Samoa'
        WHEN 'amerikanische jungferninseln'   THEN 'U.S. Virgin Islands'
        WHEN 'antigua und barbuda'            THEN 'Antigua and Barbuda'
        WHEN 'aserbaidschan'                  THEN 'Azerbaijan'
        WHEN 'australien'                     THEN 'Australia'
        WHEN 'bahamas'                        THEN 'Bahamas'
        WHEN 'barbados'                       THEN 'Barbados'
        WHEN 'belize'                         THEN 'Belize'
        WHEN 'benin'                          THEN 'Benin'
        WHEN 'bermuda'                        THEN 'Bermuda'
        WHEN 'bolivien'                       THEN 'Bolivia'
        WHEN 'bosnien und herzegowina'        THEN 'Bosnia and Herzegovina'
        WHEN 'bouvetinsel'                    THEN 'Bouvet Island'
        WHEN 'brunei darussalam'              THEN 'Brunei'
        WHEN 'bulgarien'                      THEN 'Bulgaria'
        WHEN 'burkina faso'                   THEN 'Burkina Faso'
        WHEN 'chile'                          THEN 'Chile'
        WHEN 'china'                          THEN 'China'
        WHEN 'costa rica'                     THEN 'Costa Rica'
        WHEN 'demokratische volksrepublik korea' THEN "Democratic People's Republic of Korea"
        WHEN 'dominica'                       THEN 'Dominica'
        WHEN 'dominikanische republik'        THEN 'Dominican Republic'
        WHEN 'eritrea'                        THEN 'Eritrea'
        WHEN 'estland'                        THEN 'Estonia'
        WHEN 'falklandinseln'                 THEN 'Falkland Islands'
        WHEN 'fidschi'                        THEN 'Fiji'
        WHEN 'färöer'                         THEN 'Faroe Islands'
        WHEN 'gabun'                          THEN 'Gabon'
        WHEN 'gibraltar'                      THEN 'Gibraltar'
        WHEN 'grenada'                        THEN 'Grenada'
        WHEN 'griechenland'                   THEN 'Greece'
        WHEN 'guam'                           THEN 'Guam'
        WHEN 'guatemala'                      THEN 'Guatemala'
        WHEN 'guernsey'                       THEN 'Guernsey'
        WHEN 'guinea'                         THEN 'Guinea'
        WHEN 'haiti'                          THEN 'Haiti'
        WHEN 'honduras'                       THEN 'Honduras'
        WHEN 'indonesien'                     THEN 'Indonesia'
        WHEN 'irak'                           THEN 'Iraq'
        WHEN 'iran'                           THEN 'Iran'
        WHEN 'japan'                          THEN 'Japan'
        WHEN 'jersey'                         THEN 'Jersey'
        WHEN 'jordanien'                      THEN 'Jordan'
        WHEN 'kaimaninseln'                   THEN 'Cayman Islands'
        WHEN 'kamerun'                        THEN 'Cameroon'
        WHEN 'kap verde'                      THEN 'Cape Verde'
        WHEN 'kenia'                          THEN 'Kenya'
        WHEN 'kokosinseln'                    THEN 'Cocos Islands'
        WHEN 'komoren'                        THEN 'Comoros'
        WHEN 'kuba'                           THEN 'Cuba'
        WHEN 'kuwait'                         THEN 'Kuwait'
        WHEN 'liberia'                        THEN 'Liberia'
        WHEN 'libyen'                         THEN 'Libya'
        WHEN 'liechtenstein'                  THEN 'Liechtenstein'
        WHEN 'madagaskar'                     THEN 'Madagascar'
        WHEN 'malaysia'                       THEN 'Malaysia'
        WHEN 'malta'                          THEN 'Malta'
        WHEN 'martinique'                     THEN 'Martinique'
        WHEN 'mauretanien'                    THEN 'Mauritania'
        WHEN 'mauritius'                      THEN 'Mauritius'
        WHEN 'mayotte'                        THEN 'Mayotte'
        WHEN 'mexiko'                         THEN 'Mexico'
        WHEN 'mongolei'                       THEN 'Mongolia'
        WHEN 'montenegro'                     THEN 'Montenegro'
        WHEN 'montserrat'                     THEN 'Montserrat'
        WHEN 'namibia'                        THEN 'Namibia'
        WHEN 'nepal'                          THEN 'Nepal'
        WHEN 'neukaledonien'                  THEN 'New Caledonia'
        WHEN 'neuseeland'                     THEN 'New Zealand'
        WHEN 'niederländische antillen'       THEN 'Netherlands Antilles'
        WHEN 'niue'                           THEN 'Niue'
        WHEN 'nordmazedonien'                 THEN 'North Macedonia'
        WHEN 'norfolkinsel'                   THEN 'Norfolk Island'
        WHEN 'norwegen'                       THEN 'Norway'
        WHEN 'nördliche marianen'             THEN 'Northern Mariana Islands'
        WHEN 'oman'                           THEN 'Oman'
        WHEN 'pakistan'                       THEN 'Pakistan'
        WHEN 'palau'                          THEN 'Palau'
        WHEN 'panama'                         THEN 'Panama'
        WHEN 'philippinen'                    THEN 'Philippines'
        WHEN 'pitcairn'                       THEN 'Pitcairn'
        WHEN 'polen'                          THEN 'Poland'
        WHEN 'puerto rico'                    THEN 'Puerto Rico'
        WHEN 'republik korea'                 THEN 'South Korea'
        WHEN 'rumänien'                       THEN 'Romania'
        WHEN 'salomonen'                      THEN 'Solomon Islands'
        WHEN 'saudi-arabien'                  THEN 'Saudi Arabia'
        WHEN 'schweden'                       THEN 'Sweden'
        WHEN 'senegal'                        THEN 'Senegal'
        WHEN 'serbien'                        THEN 'Serbia'
        WHEN 'serbien und montenegro'         THEN 'Serbia and Montenegro'
        WHEN 'sierra leone'                   THEN 'Sierra Leone'
        WHEN 'slowenien'                      THEN 'Slovenia'
        WHEN 'spanien'                        THEN 'Spain'
        WHEN 'sri lanka'                      THEN 'Sri Lanka'
        WHEN 'st. barthélemy'                 THEN "Saint Barthélemy"
        WHEN 'st. helena'                     THEN 'Saint Helena'
        WHEN 'sudan'                          THEN 'Sudan'
        WHEN 'svalbard und jan mayen'         THEN 'Svalbard and Jan Mayen'
        WHEN 'syrien'                         THEN 'Syria'
        WHEN 'taiwan'                         THEN 'Taiwan'
        WHEN 'thailand'                       THEN 'Thailand'
        WHEN 'togo'                           THEN 'Togo'
        WHEN 'tokelau'                        THEN 'Tokelau'
        WHEN 'trinidad und tobago'            THEN 'Trinidad and Tobago'
        WHEN 'tschad'                         THEN 'Chad'
        WHEN 'turks- und caicosinseln'        THEN 'Turks and Caicos Islands'
        WHEN 'tuvalu'                         THEN 'Tuvalu'
        WHEN 'türkei'                         THEN 'Turkey'
        WHEN 'uganda'                         THEN 'Uganda'
        WHEN 'ukraine'                        THEN 'Ukraine'
        WHEN 'ungarn'                         THEN 'Hungary'
        WHEN 'uruguay'                        THEN 'Uruguay'
        WHEN 'usbekistan'                     THEN 'Uzbekistan'
        WHEN 'vereinigtes königreich'         THEN 'United Kingdom'
        WHEN 'vietnam'                        THEN 'Vietnam'
        WHEN 'wallis und futuna'              THEN 'Wallis and Futuna'
        WHEN 'weihnachtsinsel'                THEN 'Christmas Island'
        WHEN 'zentralafrikanische republik'   THEN 'Central African Republic'
        WHEN 'zypern'                         THEN 'Cyprus'
        WHEN 'ägypten'                        THEN 'Egypt'
        WHEN 'österreich'                     THEN 'Austria'
        -- Countries already in English or non-German pass through INITCAP
        ELSE INITCAP(TRIM(m.land_region))
    END AS "BillingCountry",

    -- Legacy customer ID: same as the source key (no separate legacy system identifier)
    m.kundennummer AS "Legacy_Customer_ID__c",

    -- CreatedDate / LastModifiedDate: no source timestamps available; default to NULL
    NULL::TEXT AS "CreatedDate",
    NULL::TEXT AS "LastModifiedDate",

    -- IsDeleted: default 0 (false); no delete flag in source
    0::INTEGER AS "IsDeleted"

FROM {{ source('fixture_master_src', 'master_kunden') }} m