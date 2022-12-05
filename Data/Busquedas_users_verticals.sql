
######################################
-- Extract Busquedas, Users y Country
######################################

DECLARE start_date DATE DEFAULT '2022-09-01';
DECLARE end_date   DATE DEFAULT '2022-10-31';

CREATE OR REPLACE TABLE `peya-food-and-groceries.user_rodrigo_benitez.qc_users_0_orders_Searches`
AS

WITH header AS (


    SELECT
          fullvisitor_id as fullvisitorid
        , visit_id as visitid
        , case when S.ga_export_id = "110190327" then "Android" 
             when S.ga_export_id = "110191523" then "iOS"  
             when S.ga_export_id = "110187139" then "Web Desktop"  
             when S.ga_export_id = "110187640" then "Web Mobile" 
             else null 
            end  as platform
        , partition_date as date
        , MAX(country) as country

    FROM `peya-data-origins-pro.cl_sessions.ga_sessions`  S  
    WHERE partition_date
                BETWEEN start_date 
                       AND end_date
    GROUP BY 1,2,3,4
  )

, users AS (

  SELECT 
      DISTINCT user_id as userId

  FROM `peya-food-and-groceries.user_rodrigo_benitez.qc_users_0_orders_base`

  WHERE TRUE
    AND First_Segmentation IN ( 'Y_Food_&_N_QC'
                              , 'N_Food_&_N_QC'
                              ) 
    AND add_to_cart_dummy = 0
    AND cart_clicked_dummy = 0
    AND checkout_loaded_dummy = 0

    AND (search_profile > 0
      OR product_clicked_dummy > 0
      OR product_choice_opened_dummy > 0)
    )

, searches AS (
  SELECT 
      DATE(s.date) as date
    , s.fullVisitorId
    , s.visitId
    , s.platform
    , s.userId
    , LOWER(hp.business_name) as businessType
    , s.productSearched as search

  FROM `peya-bi-tools-pro.il_core.fact_searches_menu` s

  LEFT JOIN `peya-bi-tools-pro.il_core.dim_historical_partners` hp
    ON s.shopId = hp.restaurant_id
    AND DATE(s.date) = DATE(hp.yyyymmdd)

  WHERE DATE(date) BETWEEN start_date 
                       AND end_date

  UNION ALL

  (
  SELECT 
      DATE(n.date) as date
    , n.fullvisitorid
    , n.visitid
    , n.platform
    , h.user_id as userId
    , CASE 
        WHEN n.businessType='groceries' THEN 'market'
        ELSE n.businessType 
      END as businessType 
    ,   n.search 

  FROM `peya-food-and-groceries.automated_tables_reports.fact_nested_search_raw_data` n

  LEFT JOIN `peya-data-origins-pro.cl_sessions.ga_sessions` h 
    ON  n.fullVisitorId = h.fullvisitor_id
    AND n.visitId       = h.visit_id
    AND n.date          = h.partition_date
    AND n.platform      = h.platform

  WHERE TRUE
    AND n.search IS NOT NULL
    AND n.date BETWEEN start_date 
                   AND end_date

    AND h.partition_date BETWEEN start_date 
                             AND end_date

    )

  )

SELECT 
    s.userId
  , h.country
  , s.businessType
  , s.search

FROM searches s

INNER JOIN users u 
  ON s.userId = u.userId

LEFT JOIN header h
  ON  s.fullvisitorid = h.fullvisitorid
  AND s.visitid       = h.visitid
  AND s.platform      = h.platform
  AND s.date          = h.date