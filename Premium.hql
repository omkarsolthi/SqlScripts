set hive.cli.print.header=true; select product_id, itemid, gtin, 
regexp_extract(split(get_json_object(product_qarth_json, '$.product_attributes.primary_shelf.values[0].path_id'), ',')[1], '"(.*?)"',1) div_id, 
regexp_extract(split(get_json_object(product_qarth_json, '$.product_attributes.primary_shelf.values[0].path_id'), ',')[2], '"(.*?)"',1) prmim_id, regexp_extract(split(get_json_object(product_qarth_json, '$.product_attributes.primary_shelf.values[0].path_str'), ',')[2], '"(.*?)"',1) prmim_str, regexp_extract(split(get_json_object(product_qarth_json, '$.product_attributes.primary_category_path.values.value'), ':')[2], '"(.*?)"',1) PremiumID,  from catint.uber_flat_product_daily where date_id='20180529' and  product_identifier_type = 'PRIMARY_PRODUCT_OFFER' 