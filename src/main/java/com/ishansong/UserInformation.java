package com.ishansong;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ishansong.common.ElasticSearch6Util;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Map;


public class UserInformation {
    static TransportClient transportClient;
    static FileWriter fw;
    static BufferedWriter bfw =null;
    static JSONObject jsonObject=null;


    static{
        String hostPort = "bigdata-dev-es-1:10300,bigdata-dev-es-2:10300,bigdata-dev-es-3:10300,bigdata-dev-es-4:10300";
        try {
            transportClient = ElasticSearch6Util.getTransportClient(hostPort);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }


        try {
            fw = new FileWriter("/Users/yiqin/Desktop/20200326.txt",true);
            bfw = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
        try {
            for(int i =0; i< 7237;i++) {
                new UserInformation().ExportBulk();
                System.out.println(i);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            bfw.close();
            fw.close();
        }

    }

    public void ExportBulk() throws Exception{
        String index = "users_information";
        String type = "users_information";

        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery();
        SearchResponse response = transportClient.prepareSearch(index,type).setQuery(queryBuilder).get();

        SearchHits resultHits = response.getHits();
//        System.out.println(JSONObject.toJSON(resultHits));
        String mobile = "";
        if (resultHits.getHits().length == 0) {
            System.out.println("查到0条数据!");
        } else {
            for (int i = 0; i < resultHits.getHits().length; i++) {
                String jsonStr = resultHits.getHits()[i]
                        .getSourceAsString();

                //转成json
                jsonObject = JSON.parseObject(jsonStr);
                mobile = jsonObject.getString("mobile");
//                System.out.println(jsonStr);

                bfw.write(mobile);
                bfw.write("\n");
            }
        }
    }

    /**
     * 查询条数
     */
    private void jcj(){

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        String index = "users_information";
        String type = "users_information";

        SearchResponse searchResponse = transportClient.prepareSearch(index, type).setFrom(0).setSize(1000000).setSource(sourceBuilder).get();
        if (searchResponse != null) {
            SearchHits searchHits = searchResponse.getHits();
            long currCount = searchHits.getHits().length;
            for (int i = 0; i < currCount; i++) {
                Map<String, Object> sourceAsMap = searchHits.getAt(i).getSourceAsMap();

                //TODO 这里拿到手机号号码
                String mobile = (String) sourceAsMap.get("mobile");
            }
        }
    }

    /**
     *
     * 根据id查询某一条数据
     */
    public void searchOne(){
        String index = "users_information";
        String type = "users_information";

//        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index)
//                .setTypes(type)
//                .setFrom(0)
//                .setSize(200);

        GetResponse response = transportClient.prepareGet(index, type, "943619").get();



        String id = response.getId();

        Map<String, DocumentField> fields = response.getFields();
        // 返回的source，也就是数据源
        String mobile = (String) response.getSource().get("mobile");
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(id, mobile);
    }
}
