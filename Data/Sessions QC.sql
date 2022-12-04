
###############################################
-- Extract Datos Sesiones, Ordenes y Busquedas
###############################################

DECLARE start_date DATE DEFAULT '2022-09-01';
DECLARE end_date   DATE DEFAULT '2022-10-31';

CREATE OR REPLACE TABLE `peya-food-and-groceries.user_rodrigo_benitez.qc_users_0_orders_base`
AS

WITH searches AS (
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

, groceries_sessions AS (
  SELECT 
      v.user_id
    , v.country
    , v.businessType
    , MAX(v.shop_list_dummy)              as shop_list_dummy
    , MAX(v.shop_details_dummy)           as shop_details_dummy
    , MAX(CASE WHEN s.fullVisitorId is not null THEN 1 ELSE 0 END) as search_profile
    , MAX(v.product_clicked_dummy)        as product_clicked_dummy
    , MAX(v.product_choice_opened_dummy)  as product_choice_opened_dummy
    , MAX(v.add_to_cart_dummy)            as add_to_cart_dummy
    , MAX(v.cart_clicked_dummy)           as cart_clicked_dummy
    , MAX(v.checkout_loaded_dummy)        as checkout_loaded_dummy
    , MAX(v.transaction_dummy)            as transaction_dummy

  FROM `peya-bi-tools-pro.il_sessions.fact_sessions_funnel_by_verticals` v

  LEFT JOIN searches s
    ON  s.fullVisitorId = v.fullVisitorId
    AND s.visitId       = v.visitId
    AND DATE(s.date)    = DATE(v.partition_date)
    AND s.platform      = v.platform
    AND s.businessType  = v.businessType

  WHERE 
        v.Tribe = 'Groceries'
    AND v.partition_date BETWEEN start_date 
                             AND end_date
    AND v.user_id IS NOT NULL 
    AND v.user_id NOT IN ('0','NOT_SET','not_yet')
    AND (    v.shop_list_dummy > 0
          OR v.shop_details_dummy >0
          OR v.product_clicked_dummy >0
          OR v.product_choice_opened_dummy >0
          OR v.add_to_cart_dummy >0
          OR v.cart_clicked_dummy >0
          OR v.checkout_loaded_dummy >0
          OR v.transaction_dummy >0
        )

  GROUP BY 1,2,3
  )

, users AS (
  SELECT 
      user_id
    , customer_id
    , registered_date

  FROM `peya-bi-tools-pro.il_core.dim_user`

  WHERE 
        is_deleted = FALSE

  GROUP BY 1,2,3
  )

, customer_orders AS (
  SELECT 
      user_id
    , registered_date
    , business_type_id
    , order_id
    , nro_order_confirmed_restaurant
    , nro_order_confirmed_market
    , nro_order_confirmed_pharmacy
    , nro_order_confirmed_drinks
    , nro_order_confirmed_pets
    , nro_order_confirmed_shops
    , nro_order_confirmed_flowers
    , nro_order_confirmed_coffee
    , nro_order_confirmed_kiosks
    , row_number() over(partition by user_id order by registered_date) order_count
    , date_diff(
                date(registered_date),
                coalesce(date(lag(registered_date) over(partition by user_id order by registered_date)), date(registered_date)),
                day
                ) purchase_latency 

  FROM `peya-bi-tools-pro.il_core.fact_peya_orders_by_customers`
  WHERE 
        business_type_id NOT IN (13,11,9,0,10)
    AND registered_date_partition BETWEEN start_date 
                                      AND end_date

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
  )

, customer_orders_LT AS (
  SELECT 
      user_id
    , registered_date
    , business_type_id
    , order_id
    , nro_order_confirmed_market
    , nro_order_confirmed_pharmacy
    , nro_order_confirmed_drinks
    , nro_order_confirmed_pets
    , nro_order_confirmed_shops
    , nro_order_confirmed_flowers
    , nro_order_confirmed_kiosks

  FROM `peya-bi-tools-pro.il_core.fact_peya_orders_by_customers`
  WHERE 
        business_type_id NOT IN (1,7,13,11,9,0,10)
    AND registered_date_partition < start_date 

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
  )

, final AS (
  SELECT 
      gs.user_id
    , gs.country
    , gs.businessType
    , gs.shop_list_dummy
    , gs.shop_details_dummy
    , gs.search_profile
    , gs.product_clicked_dummy
    , gs.product_choice_opened_dummy
    , gs.add_to_cart_dummy
    , gs.cart_clicked_dummy
    , gs.checkout_loaded_dummy
    , gs.transaction_dummy

    , MAX(CASE WHEN co.nro_order_confirmed_restaurant > 0 
            OR  co.nro_order_confirmed_coffee > 0  THEN 1 ELSE 0 END) as has_Food_orders_ST

    , MAX(CASE WHEN co.nro_order_confirmed_market > 0 
            OR  co.nro_order_confirmed_pharmacy > 0  
            OR  co.nro_order_confirmed_drinks > 0 
            OR  co.nro_order_confirmed_pets > 0
            OR  co.nro_order_confirmed_shops > 0
            OR  co.nro_order_confirmed_flowers > 0 
            OR  co.nro_order_confirmed_kiosks > 0   THEN 1 ELSE 0 END) as has_QC_orders_ST

    , MAX(CASE WHEN co_lt.nro_order_confirmed_market > 0 
            OR  co_lt.nro_order_confirmed_pharmacy > 0  
            OR  co_lt.nro_order_confirmed_drinks > 0 
            OR  co_lt.nro_order_confirmed_pets > 0
            OR  co_lt.nro_order_confirmed_shops > 0
            OR  co_lt.nro_order_confirmed_flowers > 0 
            OR  co_lt.nro_order_confirmed_kiosks > 0   THEN 1 ELSE 0 END) as has_QC_orders_LT

    , MAX(CASE WHEN co_lt.nro_order_confirmed_market > 0 THEN 1 ELSE 0 END) as has_Market_orders_LT

  FROM groceries_sessions gs

  LEFT JOIN users us
    ON gs.user_id = CAST(us.user_id AS STRING)

  LEFT JOIN customer_orders co          --------> Ordenes de TODOS en el PRESENTE
    ON gs.user_id = CAST(co.user_id AS STRING)

  LEFT JOIN customer_orders_LT co_lt    --------> Ordenes de QC el en PASADO
    ON gs.user_id = CAST(co_lt.user_id AS STRING)

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
  )

SELECT 
      f.*
  , CASE 
        WHEN has_Food_orders_ST = 1 AND has_QC_orders_ST = 1 THEN 'Y_Food_&_Y_QC'
        WHEN has_Food_orders_ST = 1 AND has_QC_orders_ST = 0 THEN 'Y_Food_&_N_QC'   
        WHEN has_Food_orders_ST = 0 AND has_QC_orders_ST = 0 THEN 'N_Food_&_N_QC'   
        WHEN has_Food_orders_ST = 0 AND has_QC_orders_ST = 1 THEN 'N_Food_&_Y_QC'   
    END as First_Segmentation

FROM final f

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
