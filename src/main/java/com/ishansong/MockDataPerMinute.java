package com.ishansong;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ishansong.common.ElasticSearch6Util;
import com.ishansong.model.PerMinute;
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

public class MockDataPerMinute {

    private static TransportClient transportClient;
    private static BulkRequestBuilder bulkRequest;
    private static String time = "1572105600000";//30号
    private static String id;
    private static int place=0;
    private static int receive=0;
    private static int finish=0;
    static FileWriter fw;


    static{
        String hostPort = "bigdata-dev-es-1:10300,bigdata-dev-es-2:10300,bigdata-dev-es-3:10300,bigdata-dev-es-4:10300";
        try {
            transportClient = ElasticSearch6Util.getTransportClient(hostPort);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        try {
            fw = new FileWriter("/Users/yiqin/Desktop/20191027_3301.txt",true);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    private static String getPerMinute(){
        Random random = new Random();
        PerMinute perMinute = new PerMinute();
        perMinute.setCityId("3301");
        perMinute.setMinuter(getTime(time));
        id = "3301#"+getTime(time);
        System.out.println(id);
        time=String.valueOf(Long.valueOf(time)+60000);
        int temp =random.nextInt(40)+40;
        place = place + temp;
        perMinute.setPlace(place);

        receive = receive+temp-random.nextInt(10);
        perMinute.setReceive(receive);

        writeToTxt(String.valueOf(place),String.valueOf(receive));
        finish = finish+temp-random.nextInt(5);
        perMinute.setFinish(finish);
        perMinute.setCanel(1l);
        return JSON.toJSONString(perMinute, SerializerFeature.WriteMapNullValue);

    }


    public static String getTime(String time){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd#HH:mm:ss");
        String date = df.format(new Date(Long.valueOf(time)));
        return date;
    }



    public static void main(String[] args) {
//        String index = "cumulative_order_per_minute_201910";
        String index = "cumulative_order_per_minute_offline_201910";
        String type = "cumulative_order_per_minute_type";

//        bulkRequest = transportClient.prepareBulk();

        long a = System.currentTimeMillis(); //开始时间

        for(int i=0;i<1440;i++){
            transportClient.prepareIndex(index, type)
                    .setSource(getPerMinute(), XContentType.JSON)
                    .setId(id)
                    .get();
        }

        if (transportClient != null) {
            transportClient.close();
        }

        long b = System.currentTimeMillis();

        System.out.println(b-a);

    }


    public static void writeToTxt(String place,String receive){
        PrintWriter pw = new PrintWriter(fw);
        pw.print(place);
        pw.print(",");
        pw.print(receive);
        pw.println();
        pw.flush();
    }


}
