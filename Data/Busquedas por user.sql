
################################
-- Cantidad de busquedas hechas 
################################

SELECT 
    First_Segmentation
  , CASE 
      WHEN (search_profile > 0
        OR product_clicked_dummy > 0
        OR product_choice_opened_dummy > 0) THEN 1
      ELSE 0 END as product_interaction
  , count(DISTINCT user_id)
FROM `peya-food-and-groceries.user_rodrigo_benitez.qc_users_0_orders_base`

WHERE TRUE
  AND First_Segmentation IN ( 'Y_Food_&_N_QC'
                            , 'N_Food_&_N_QC'
                            ) 
  AND add_to_cart_dummy = 0
  AND cart_clicked_dummy = 0
  AND checkout_loaded_dummy = 0
    

GROUP BY 1,2