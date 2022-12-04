
################################
-- Cantidad de busquedas hechas 
################################

SELECT 
    First_Segmentation
  , search_profile as search_profile
  , count(DISTINCT user_id)
FROM `peya-food-and-groceries.user_rodrigo_benitez.qc_users_0_orders_base`

WHERE TRUE
  AND First_Segmentation IN ( 'Y_Food_&_N_QC'
                            , 'N_Food_&_N_QC'
                            ) 
  AND (
          product_clicked_dummy = 0
      OR  product_choice_opened_dummy = 0
      OR  add_to_cart_dummy = 0
      OR  cart_clicked_dummy = 0
      OR  checkout_loaded_dummy = 0
    )

GROUP BY 1,2