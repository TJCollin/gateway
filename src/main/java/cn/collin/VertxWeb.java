package cn.collin;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.function.Consumer;

/**
 * Created by collin on 17-5-3.
 */
public class VertxWeb extends AbstractVerticle{

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        // 部署发布rest服务
        vertx.deployVerticle(new VertxWeb());
//        VertxOptions
    }

    @Override
    public void start() throws Exception {
        final Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.post("/hello").handler(this::handlePost);
        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }

    private void handlePost(RoutingContext context) {
//         param = Json.decodeValue(context.getBodyAsString())
        System.out.println("context = [" + context.getBodyAsJson().getString("a") + "]");
        context.response().putHeader("content-type", "text").end("hello!");
    }

}
