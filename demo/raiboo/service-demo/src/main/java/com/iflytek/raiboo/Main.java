package com.iflytek.raiboo;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import java.io.IOException;
import java.net.URI;

/**
 * Main class.
 */
public class Main {
    // Base URI the Grizzly HTTP server will listen on
//    public static final String BASE_URI = "http://172.16.60.75:19347/";
//        public static final String BASE_URI = "http://localhost:8080/";
    public static final String BASE_URI = "http://172.16.60.212:19347/";


    /**
     * Starts Grizzly HTTP server exposing JAX-RS resources defined in this application.
     * java -cp service-demo-1.0-SNAPSHOT-jar-with-dependencies.jar com.iflytek.raiboo.Main
     *
     * @return Grizzly HTTP server.
     */
    public static HttpServer startServer() {
        // create a resource config that scans for JAX-RS resources and providers
        // in com.example package
        final ResourceConfig rc = new ResourceConfig().packages("com.iflytek.raiboo");
        rc.register(new CRFilter());

        // create and start a new instance of grizzly http server
        // exposing the Jersey application at BASE_URI
        return GrizzlyHttpServerFactory.createHttpServer(URI.create(BASE_URI), rc);
    }

    /**
     * Main method.
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {


        final HttpServer server = startServer();
        System.out.println(String.format("Jersey app started with WADL available at "
                + "%sapplication.wadl\nHit enter to stop it...", BASE_URI));
        System.in.read();
        server.stop();
    }
}

