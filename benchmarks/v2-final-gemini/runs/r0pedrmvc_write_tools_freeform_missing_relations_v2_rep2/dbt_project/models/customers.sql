{{ config(materialized='table') }}

SELECT
    id AS "CustomerId",
    name AS "CustomerName",
    'my_source' AS "Legacy_Source__c",
    CAST(id AS VARCHAR) AS "Legacy_Customer_Id__c"
FROM {{ source('my_source', 'some_table') }}
