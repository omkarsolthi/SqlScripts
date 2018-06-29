insert into table ogoud.item_trends select date_id, product_identifier_type, Pub_sts, count(distinct product_id) pcnt, count(offer_id) offr_cnt, count(barcode) top, sum(putcnt) PUT_cnt, sum(s2hcnt) S2H_cnt, sum(s2scnt) S2S_cnt, FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd') datelocal from (select a.date_id, a.product_id, a.offer_id, a.product_identifier_type, t.barcode, a.Pub_sts, max(a.PUT) putcnt, max(a.S2H) s2hcnt, max(a.S2S) s2scnt from (select  distinct u.product_id, u.date_id, u.offer_id, u.product_identifier_type, get_json_object(u.product_offer_panama_json, '$.offer.productId.upc') UPC, get_json_object(u.product_offer_json,'$.offerPublishStatus') Pub_sts, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isPutEligible')='true' then 1 else 0 end as PUT, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isS2HEligible')='true' then 1 else 0 end as S2H, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isS2SEligible')='true' then 1 else 0 end as S2S from catint.uber_flat_product_daily u  where u.date_id=${hiveconf:yday} and u.product_identifier_type!='PRIMARY_PRODUCT_OFFER' and get_json_object(product_offer_json,'$.offerLifecycleStatus')='ACTIVE') a left outer join (select barcode from jetanalytics_assortment.top_mm_go_get where is_top_1m = 1) t on a.UPC=t.barcode group by a.date_id, a.product_id, a.offer_id, t.barcode, a.product_identifier_type, a.Pub_sts) b group by date_id, product_identifier_type, Pub_sts;

 	

 	(select  distinct u.product_id, u.date_id, u.offer_id, u.product_identifier_type, get_json_object(u.product_offer_panama_json, '$.offer.productId.upc') UPC, get_json_object(u.product_offer_json,'$.offerPublishStatus') Pub_sts, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isPutEligible')='true' then 1 else 0 end as PUT, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isS2HEligible')='true' then 1 else 0 end as S2H, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isS2SEligible')='true' then 1 else 0 end as S2S from catint.uber_flat_product_daily u  where u.date_id='20180413' and u.product_identifier_type!='PRIMARY_PRODUCT_OFFER' and get_json_object(product_offer_json,'$.offerLifecycleStatus')='ACTIVE')



export MAPR_MAPREDUCE_MODE=yarn;
hive -e "set hive.cli.print.header=true;select count(p.product_id) cnt from (select product_id, get_json_object(product_offer_panama_json, '$.offer.productId.upc') UPC from catint.uber_flat_product_daily where date_id='20180413' and product_publish_status='PUBLISHED' and product_lifecycle_status='ACTIVE' and product_type='PRIMARY_PRODUCT_OFFER') p join jetanalytics_assortment.top_mm_go_get t on t.barcode=p.UPC where t.is_top_1m = 1" > 1MpCnt.txt


export MAPR_MAPREDUCE_MODE=yarn;
hive -e "set hive.cli.print.header=true; select count(product_id) from catint.uber_flat_product_daily p join jetanalytics_assortment.top_mm_go_get t on t.barcode=p.UPC where t.is_top_1m = 1 and p.date_id='20180413' and p.product_publish_status='PUBLISHED' and p.product_lifecycle_status='ACTIVE' and p.product_identifier_type = 'PRIMARY_PRODUCT_OFFER' " > test_txt2.txt






export MAPR_MAPREDUCE_MODE=yarn;
hive -e "set hive.cli.print.header=true; select a.product_id, a.date_id, a.upc, a.pub_sts,  sum(a.1p) 1pocnt, sum(a.3p) 3pocnt, max(a.1p) 1pcnt, max(a.3p) 3pcnt, max(a.put) putcnt, max(a.s2h) s2hcnt, max(a.s2s) s2scnt from (select  u.product_id, u.date_id, case when u.product_identifier_type='DOTCOM_OFFER' then 1 else 0 end as 1p, case when u.product_identifier_type='SELLER_OFFER' then 1 else 0 end as 3p,  get_json_object(u.product_offer_panama_json, '$.offer.productId.upc') UPC, get_json_object(u.product_offer_json,'$.offerPublishStatus') Pub_sts, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isPutEligible')='true' then 1 else 0 end as PUT, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isS2HEligible')='true' then 1 else 0 end as S2H, case when u.product_identifier_type='DOTCOM_OFFER' and get_json_object(u.oscar_json,'$.result.offerAttributes.entry.logistics.isS2SEligible')='true' then 1 else 0 end as S2S from catint.uber_flat_product_daily u  where u.date_id='20180415' and u.product_identifier_type!='PRIMARY_PRODUCT_OFFER' and get_json_object(u.product_offer_json,'$.offerLifecycleStatus')='ACTIVE') a group by a.product_id, a.date_id, a.upc, a.pub_sts limit 5000" >1mm_test02.xls;




select u.product_id, u.offer_id, u.product_identifier_type from catint.uber_flat_product_daily u where u.date_id='20180415' and u.product_identifier_type!='PRIMARY_PRODUCT_OFFER' and get_json_object(u.product_offer_json,'$.offerLifecycleStatus')='ACTIVE' 









