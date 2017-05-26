package cn.collin.kafka;

import cn.collin.webServer.VertxWeb;
import kafka.utils.ShutdownableThread;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * Created by collin on 17-5-4.
 */
public class Consumer extends ShutdownableThread{

    private final KafkaConsumer<String, String > consumer;
    private final String topic;
    private TopicPartition topicPartition = new TopicPartition(KafkaProperties.TOPIC, 0);
    private ConsumerRecords<String, String> records;
    private Map<Integer, String> sortMap = new TreeMap<>();
    private String indexData;
    private JSONArray jsonArray = new JSONArray();

    public Consumer(String topic) {
        super("KafkaConsumerExample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
        this.topic = topic;
    }

    @Override
    public void doWork() {
        consumer.subscribe(Collections.singletonList(this.topic));
        records = consumer.poll(1000);
        /*for (ConsumerRecord<String, String > record  : records){
            System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
        }*/
//        records.records()
        List<ConsumerRecord<String, String>> list = records.records(topicPartition);
        System.out.println("listSize:"+list.size());
        consumer.close();
        for (int i=0; i<list.size()-1; i++){
            for (int j=i+1; j<list.size(); j++){
                for (int k=j+1; k<list.size(); k++){
                    String key1 = list.get(i).key();
                    String key2 = list.get(j).key();
                    String key3 = list.get(k).key();
                    if (key1.equals(key2) && key2.equals(key3)){
                        int d1 = getDataType(list.get(i).value());
                        int d2 = getDataType(list.get(j).value());
                        int d3 = getDataType(list.get(k).value());
                        sortMap.put(d1, list.get(i).value());
                        sortMap.put(d2, list.get(j).value());
                        sortMap.put(d3, list.get(k).value());

                        for (Iterator<Integer> it = sortMap.keySet().iterator(); it.hasNext();) {
                            int key = it.next();
                            String value = sortMap.get(key);
                            jsonArray.add(key, value);
                        }
//                        System.out.println(jsonArray.toString());
                        this.indexData = composeIndex(jsonArray);
                        createESIndex(indexData);


                        /*int d1 = getDataType(list.get(i).value());
                        int d2 = */
                    }
                }

                /*if (list.get(i).key().equals(list.get(j).key())){
                    String serverId1 = getServerId(list.get(i).value());
                    String serverId2 = getServerId(list.get(j).value());
                    int dataType = getDataType(list.get(i).value());
                    if (serverId1.equals(serverId2)){
//                        System.out.println("success");
                        if (dataType == 0) {
                            this.indexData = composeIndex(list.get(i).value(), list.get(j).value());
                        } else {
                            this.indexData = composeIndex(list.get(j).value(), list.get(i).value());
                        }

                        System.out.println("indexData:"+indexData);
                        createESIndex(indexData);
                        System.out.println(indexData);
                    } else {
                        this.indexData = "lost data";
                    }
                } else {
                    indexData = "lost data";
                }*/
            }

        }
        /*for (int i=0; i<list.size()-1; i++){
            for (int j=i+1; j<list.size(); j++){
                if (list.get(i).key().equals(list.get(j).key())){
                    String serverId1 = getServerId(list.get(i).value());
                    String serverId2 = getServerId(list.get(j).value());
                    int dataType = getDataType(list.get(i).value());
                    if (serverId1.equals(serverId2)){
//                        System.out.println("success");
                        if (dataType == 0) {
                            this.indexData = composeIndex(list.get(i).value(), list.get(j).value());
                        } else {
                            this.indexData = composeIndex(list.get(j).value(), list.get(i).value());
                        }

                        System.out.println("indexData:"+indexData);
                        createESIndex(indexData);
                        System.out.println(indexData);
                    } else {
                        this.indexData = "lost data";
                    }
                } else {
                    indexData = "lost data";
                }
            }

        }*/

    }

    public int getDataType(String val) {
        int dataType = JSONObject.fromObject(val).getInt("dataType");
        return dataType;
    }


    public String composeIndex (JSONArray jsonArray){
        JSONObject val1 = jsonArray.getJSONObject(0);
        JSONObject val2 = jsonArray.getJSONObject(1);
        JSONObject val3 = jsonArray.getJSONObject(2);

        String serverId = val1.getString("serverId");
        String invokeId = val2.getString("invokeId");
        String chaincodeId = val1.getString("chaincodeId");
//        String dataType = JSONObject.fromObject(val)
        Long startTime = val1.getLong("timestamp");
        Long endTime = val2.getLong("timestamp");
        Long checkTime = val3.getLong("timestamp");
        String result = JSONObject.fromObject(val2).getString("result");
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("serverId", serverId);
        jsonObject.put("chaincodeId", chaincodeId);
        jsonObject.put("invokeId", invokeId);
        jsonObject.put("startTime", startTime);
        jsonObject.put("endTime", endTime);
        jsonObject.put("checkTime", checkTime);
        jsonObject.put("result", result);
        return jsonObject.toString();
    }

    /*public String consumeData (){
        run();
        return indexData;
    }*/

    public void createESIndex (String data) {
//        Vertx vertx = Vertx.vertx();
        VertxWeb.vertx.createHttpClient().post(9200, "localhost", "/chain/code/", resp -> {
//            System.out.println("Got response " + resp.statusCode());
            resp.bodyHandler(body -> {
//                System.out.println("Got data " + body.toString("utf-8"));
            });
        }).end(data);
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public boolean isInterruptible() {
        return false;
    }
}
