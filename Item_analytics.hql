

Test line 
Insert into table ogoud.item_analytics partition (cre_dt='2018-03-01') select date_id, product_identifier_type, get_json_object(product_offer_json,'$.offerPublishStatus') Pub_sts, get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isPutEligible') PUT, get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isS2HEligible') S2H, get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isS2SEligible') S2S, count(distinct product_id) as prod_cnt from catint.uber_flat_product_daily where date_id='20180307' and get_json_object(product_offer_json,'$.offerLifecycleStatus')='ACTIVE' and product_identifier_type<>'Primary_Product_offer' group by date_id, product_identifier_type, get_json_object(product_offer_json,'$.offerPublishStatus'), get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isPutEligible'), get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isS2HEligible'), get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isS2SEligible') limit 5;





FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd')



insert into table ogoud.item_analytics partition (cre_dt='2018-03-01') select date_id, product_identifier_type, get_json_object(product_offer_json,'$.offerPublishStatus') Pub_sts, get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isPutEligible') PUT, get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isS2HEligible') S2H, get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isS2SEligible') S2S, count(distinct product_id) as prod_cnt from catint.uber_flat_product_daily where date_id='20180307' and get_json_object(product_offer_json,'$.offerLifecycleStatus')='ACTIVE' and product_identifier_type<>'Primary_Product_offer' group by date_id, product_identifier_type, get_json_object(product_offer_json,'$.offerPublishStatus'), get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isPutEligible'), get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isS2HEligible'), get_json_object(oscar_json,'$.result.offerAttributes.entry.logistics.isS2SEligible') limit 5;
