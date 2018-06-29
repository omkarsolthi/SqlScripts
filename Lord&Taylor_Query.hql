select p.product_id, p.product_publish_status, p.product_class_type, p.itemid, p.gtin, get_json_object(p.product_qarth_json, '$.product_attributes.sku.values[0].value') SKUvalue, get_json_object(p.product_qarth_json, '$.product_attributes.model.values[0].value') MODELvalue, get_json_object(p.product_qarth_json, '$.product_attributes.ironbank_category.values[0].source_value') IronBank_Category, get_json_object(p.product_qarth_json, '$.meta.variant_group_id') variant_group_id, get_json_object(p.product_qarth_json, '$.meta.product_setup_type') product_setup_type, regexp_extract(split(get_json_object(p.product_qarth_json, '$.product_attributes.reporting_hierarchy.values[0].path_str'), ',')[0], '"(.*?)"',1) division, regexp_extract(split(get_json_object(p.product_qarth_json, '$.product_attributes.reporting_hierarchy.values[0].path_str'), ',')[3], '"(.*?)"',1) category from catint.uber_flat_product_daily p join catint.uber_flat_product_daily o on p.product_id=o.product_id where p.date_id='20180524' and o.date_id='20180524' and p.product_publish_status = 'PUBLISHED' and p.product_identifier_type = 'PRIMARY_PRODUCT_OFFER' and get_json_object(p.product_qarth_json, '$.meta.org_id')='62cf9baf-5833-4da4-8dac-82dc01b137f5' 