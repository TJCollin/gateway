package cn.collin.webServer;
import cn.collin.connDB.ConnDB;
import cn.collin.kafka.*;
import com.sun.org.apache.regexp.internal.RE;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by collin on 17-5-3.
 */
public class VertxWeb extends AbstractVerticle{
    private HttpServer httpServer;
    static ServerWebSocket webSocket;
    private String messageKey;
    private String messageStr;
    private Producer producer;
    private Consumer consumer;
    private String serverId = "";
    private Long searchStart;
    private Long searchEnd;
    private String ESURL = "http://localhost:9200/";
    public static Vertx vertx;
    Connection conn= ConnDB.getConn();
    Statement stmt=null;
    ResultSet rs=null;
    String sql="";

    public static void main(String[] args) {
        vertx = Vertx.vertx();
        // 部署发布rest服务
        vertx.deployVerticle(new VertxWeb());
//        VertxOptions
    }

    @Override
    public void start() throws Exception {
        httpServer = vertx.createHttpServer();
        final Router router = Router.router(vertx);
        router.route().handler(CorsHandler.create("*")
                .allowedMethod(HttpMethod.GET)
                .allowedMethod(HttpMethod.POST)
                .allowedMethod(HttpMethod.OPTIONS)
                .allowedHeader("X-PINGARUNER")
                .allowedHeader("Content-Type"));
        router.route().handler(BodyHandler.create());
        /*router.post("/invokeStart").handler(this::startInvoke);
        router.post("/invokeEnd").handler(this::endInvoke);
        router.get("/testEnd").handler(this::endTest);
        router.post("/esData").handler(this::searchES);*/
        router.post("/searchResult").handler(this::searchResult);
        router.post("/invoke").handler(this::invoke);
//        router.post("/endInvoke").handler(this::endInvoke);
        router.get("/endTest").handler(this::endTest);
//        router.post("/getData").handler(this::getData);
        httpServer.requestHandler(router::accept).listen(8080);

        stmt=conn.createStatement();
//        httpServer.websocketHandler(this::handleSocket).listen(8086);
    }

    private void searchResult(RoutingContext context) {
        context.response().end("ok");
        messageKey = context.getBodyAsJson().getString("id");
        messageStr = context.getBodyAsString();
        insertData(messageStr);
        producer = new Producer(KafkaProperties.TOPIC, true, messageStr, messageKey);
        producer.run();
//        System.out.println("context = [" + context.getBodyAsString() + "]");
    }


    private void invoke (RoutingContext context) {
        context.response().end("ok");
//        System.out.println(context.getBodyAsString());
        messageKey = context.getBodyAsJson().getString("id");
        messageStr = context.getBodyAsString();
        insertData(messageStr);
        producer = new Producer(KafkaProperties.TOPIC, true, messageStr, messageKey);
        producer.run();
//        producer = new Producer(KafkaProperties.TOPIC2,false, messageStr, messageKey);
        SparkProducer sparkProducer = new SparkProducer(KafkaProperties.TOPIC2, false,messageStr,messageKey);
        sparkProducer.run();
//         param = Json.decodeValue(context.getBodyAsString())
//        System.out.println("context = [" + context.getBodyAsJson().getLong("id") + "]");
//        System.out.println("context = [" + context.getBodyAsString() + "]");

    }

    private void endTest (RoutingContext context) {
        context.response().end("ok");
        consumer = new Consumer(KafkaProperties.TOPIC);
        consumer.run();

        /*vertx.createHttpClient().getNow(9200, "localhost", "/", resp -> {
            System.out.println("Got response " + resp.statusCode());
            resp.bodyHandler(body -> {
                System.out.println("Got data " + body.toString("utf-8"));
            });
        });*/
    }

    public void insertData(String s) {
        try {
            JSONObject jsonObject = JSONObject.fromObject(s);
            String invokeId = jsonObject.getString("invokeId");

            sql = "insert into task(invoke_id, data) values (\'"+invokeId+"\',\'"+s+"\')";
//            System.out.println(sql);
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
