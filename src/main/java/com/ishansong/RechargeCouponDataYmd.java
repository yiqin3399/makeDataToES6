package com.ishansong;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ishansong.common.ElasticSearch6Util;
import com.ishansong.model.PerMinute;
import com.ishansong.model.RechargeCoupon;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class RechargeCouponDataYmd {
    private static TransportClient transportClient;
    private static BulkRequestBuilder bulkRequest;
    static FileWriter fw;
    static RechargeCoupon rechargeCoupon ;


    static{
        String hostPort = "bigdata-dev-es-1:10300,bigdata-dev-es-2:10300,bigdata-dev-es-3:10300,bigdata-dev-es-4:10300";
        try {
            transportClient = ElasticSearch6Util.getTransportClient(hostPort);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }



    }



    private static String getRechargeCoupon(){
        Random random = new Random();
        rechargeCoupon =  new RechargeCoupon();
        rechargeCoupon.setUnid(UUID.randomUUID().toString().replaceAll("-",""));
        rechargeCoupon.setActivity_group_id(0);
        rechargeCoupon.setActivity_group_name("充值返券");
        rechargeCoupon.setActivity_id(19227);
        rechargeCoupon.setActivity_name("常规1遂宁充值1千（20190804)");
        rechargeCoupon.setActivity_status(0);
        rechargeCoupon.setStart_time("2019-08-04T20:41:05.247+08:00");
        rechargeCoupon.setEnd_time("2050-01-01T00:00:00+08:00");
        rechargeCoupon.setPer_recharge_amt(100000);
        rechargeCoupon.setCoupon_type("22609");
        rechargeCoupon.setCoupon_type_name("折扣卡");
        rechargeCoupon.setCoupon_amt(random.nextInt(80)+80);
        rechargeCoupon.setRev_coupon_cnt(0);
        rechargeCoupon.setExpire_days(31);
        rechargeCoupon.setRev_cnt(0);
        rechargeCoupon.setPer_assign_count(random.nextInt(100)+100);
        rechargeCoupon.setCoupon_user_cnt(random.nextInt(100)+6);
        rechargeCoupon.setCoupon_exp_cnt(random.nextInt(10)+10);
        rechargeCoupon.setCoupon_able_cnt(random.nextInt(1000)+1000);
        rechargeCoupon.setCoupon_finish_cnt(random.nextInt(100)+20);
        rechargeCoupon.setCoupon_finish_amt(12000);
        rechargeCoupon.setCoupon_discount_amt(1628);
        rechargeCoupon.setEtl_time("2020-05-15T10:28:07+08:00");
        rechargeCoupon.setCity_id(5109);
        rechargeCoupon.setCity_name("遂宁市");
        long time = random.nextInt(7776000)+1577808615;
        rechargeCoupon.setDt_ymd(getDtymd(String.valueOf(time*1000)));

        return JSON.toJSONString(rechargeCoupon, SerializerFeature.WriteMapNullValue);

    }


    public static String getDtymd(String time){
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        String date = df.format(new Date(Long.valueOf(time)));
        return date;

    }





    public static void main(String[] args) {
//        String index = "cumulative_order_per_minute_201910";
        String index = "dm_recharge_coupon_data_ymd";
        String type = "dm_recharge_coupon_data";


        bulkRequest = transportClient.prepareBulk();

        long a = System.currentTimeMillis(); //开始时间

        int num = 0;
        for(int i=0; i< 50000;i++) {//总共往ES里放的数据量，2万条

            bulkRequest.add(transportClient.prepareIndex(index, type)
                    .setSource(getRechargeCoupon(), XContentType.JSON)
                    .setId(rechargeCoupon.getUnid()));
            num ++;
            if (num >= 1000) {//1次1000条
                if (bulkRequest.numberOfActions() > 0) {
//                    bulkRequest.get();
                    bulkRequest.execute().actionGet();
                }
                num = 0;
                bulkRequest = transportClient.prepareBulk();
            }

        }

        if (bulkRequest.numberOfActions() > 0) {
//            bulkRequest.get();
            bulkRequest.execute().actionGet();
        }


        long b = System.currentTimeMillis();

        System.out.println(b-a);


        }

//        long a = System.currentTimeMillis(); //开始时间
////
////        for(int i=0;i<10;i++){
////            transportClient.prepareIndex(index, type)
////                    .setSource(getRechargeCoupon(), XContentType.JSON)
////                    .setId(id)
////                    .get();
////        }
////
////        if (transportClient != null) {
////            transportClient.close();
////        }
////
////        long b = System.currentTimeMillis();
////
////        System.out.println(b-a);

//    }


//    public static void writeToTxt(String place,String receive){
//        PrintWriter pw = new PrintWriter(fw);
//        pw.print(place);
//        pw.print(",");
//        pw.print(receive);
//        pw.println();
//        pw.flush();
//    }

}
