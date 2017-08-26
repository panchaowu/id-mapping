package dmp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;

/**
 * Created by taochen4 on 2017/2/17.
 */
public class HttpServer {
    private static Logger logger = Logger.getLogger(HttpServer.class);
    private int _port = 8080;
    private String _host = "127.0.0.1";
    public static boolean _isSSL = false;
    private static final String DEFAULT_URL = "";

    public HttpServer(String host, int port) {
        this._host = host;
        this._port = port;
    }

    public void run() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new HttpServerInitializer());
            ChannelFuture future = serverBootstrap.bind(_host, _port).sync();
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Parameters error.");
        }

        logger.info("Netty IP:" + args[0] + " PORT:" + args[1]);
        new HttpServer(args[0], Integer.valueOf(args[1])).run();
        logger.info("Netty Server stop");
    }
}
