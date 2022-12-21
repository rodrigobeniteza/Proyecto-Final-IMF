CREATE OR REPLACE TABLE `peya-food-and-groceries.user_rodrigo_benitez.product_categories`
AS

WITH master_product as (
  
  SELECT
    piece_barcode master_product_code,
    category_level_one,
    category_level_two,
    category_level_three,

  FROM(
      SELECT   
          Row_number() over (partition BY ltrim(piece_barcode,'0') ORDER BY global_entity_id DESC, vat_rate DESC, master_product_id DESC, barcode_id DESC, code DESC) AS masterprodnumber,
          product_type,
          piece_barcode,
          master_categories.level_one as category_level_one,
          master_categories.level_two as category_level_two,
          master_categories.level_three as category_level_three,

      FROM `peya-data-origins-pro.cl_catalogue.master_product`

      WHERE 
            piece_barcode IS NOT NULL
        AND title IS NOT NULL
      ORDER BY 
          piece_barcode,
          masterprodnumber)

  WHERE masterprodnumber = 1 

  ORDER BY 
      piece_barcode)


SELECT 
    m.partnerId as partner_id
  , dp.partner_name
  , dp.partner_description
  , dp.business_type_id
  , dp.business_type.business_type_name
  , dp.country_id
  , s.legacyId as legacyId_section
  , s.name as section_name
  , p.legacyId as product_legacy_id
  , p.Id as product_id
  , p.name as product_name
  , pd.product_description
  , pd.gtin
  , concat(mp.category_level_one, ' | ', mp.category_level_two, ' | ', IFNULL(mp.category_level_three,'')) category_level_1_2_3

FROM `peya-bi-tools-pro.il_core.dim_partner` dp

INNER JOIN `peya-bi-tools-pro.il_core.dim_partner_menu` m
  ON dp.partner_id = m.partnerid

INNER JOIN  UNNEST(section) s 
INNER JOIN UNNEST(product) p

LEFT JOIN (
    SELECT 
        legacyId product_legacyId
      , description as product_description
      , LPAD(gtin, 14, '0') as gtin
      
    FROM `peya-data-origins-pro.cl_core.is_product` p
    LEFT JOIN UNNEST (legacyIds) legacyId
    
    WHERE description is not null
    
    GROUP BY 1,2,3 ) pd 
  ON pd.product_legacyId = p.legacyId

LEFT JOIN master_product mp
  ON pd.gtin = mp.master_product_code

WHERE 
      dp.partner_status = 'ON_LINE' 
  AND business_type_id	IN (2)

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14