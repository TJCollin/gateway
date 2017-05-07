package cn.collin.webServer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import kafka.Consumer;
import kafka.KafkaProperties;
import kafka.Producer;

/**
 * Created by collin on 17-5-3.
 */
public class VertxWeb extends AbstractVerticle{
    private String messageKey;
    private String messageStr;
    private Producer producer;
    private Consumer consumer;
    private String ESURL = "http://localhost:9200/";
    public static Vertx vertx;

    public static void main(String[] args) {
        vertx = Vertx.vertx();
        // 部署发布rest服务
        vertx.deployVerticle(new VertxWeb());
//        VertxOptions
    }

    @Override
    public void start() throws Exception {
        final Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.post("/startInvoke").handler(this::startInvoke);
        router.post("/endInvoke").handler(this::endInvoke);
        router.get("/endTest").handler(this::endTest);
        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }

    private void startInvoke (RoutingContext context) {
        messageKey = context.getBodyAsJson().getString("id");
        messageStr = context.getBodyAsString();
        producer = new Producer(KafkaProperties.TOPIC, true, messageStr, messageKey);
        producer.run();
//         param = Json.decodeValue(context.getBodyAsString())
        System.out.println("context = [" + context.getBodyAsJson().getString("id") + "]");
//        System.out.println("context = [" + context.getBodyAsString() + "]");
        context.response().end("hello!");
    }

    private void endInvoke (RoutingContext context) {
        messageKey = context.getBodyAsJson().getString("id");
        messageStr = context.getBodyAsString();
        producer = new Producer(KafkaProperties.TOPIC, true, messageStr, messageKey);
        producer.run();
        System.out.println("context = [" + context.getBodyAsJson().getString("id") + "]");
//        System.out.println("context = [" + context.getBodyAsString() + "]");
        context.response().end("hello!");

    }

    private void endTest (RoutingContext context) {
//        System.out.println("context = [" + context.getBodyAsJson().getString("id") + "]");
        System.out.println("context = []");
        consumer = new Consumer(KafkaProperties.TOPIC);
        consumer.run();
//        String indexData = consumer.consumeData();
//        System.out.println("indexData:"+indexData);
        context.response().end("hello!");

        /*vertx.createHttpClient().getNow(9200, "localhost", "/", resp -> {
            System.out.println("Got response " + resp.statusCode());
            resp.bodyHandler(body -> {
                System.out.println("Got data " + body.toString("utf-8"));
            });
        });*/
    }

}
