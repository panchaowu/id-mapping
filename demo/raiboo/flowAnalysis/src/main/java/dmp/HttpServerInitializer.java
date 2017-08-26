package dmp;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

/**
 * Created by taochen4 on 2017/2/17.
 */
public class HttpServerInitializer extends ChannelInitializer<SocketChannel> {


    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
//        System.out.println("Initializing Http Server channel...");
        ChannelPipeline pipeline = socketChannel.pipeline();

//        if (HttpServer._isSSL) {
//
//        }

        /**
         * http-request解码器
         * http服务器端对request解码
         */
        pipeline.addLast("http-decoder", new HttpRequestDecoder());

        /**
         * http-aggregator"解码器
         * http服务器端将多个消息转换为单一的FullHttpRequest或FullHttpResponse
         */
        pipeline.addLast("http-aggregator", new HttpObjectAggregator(512 * 1024));

        /**
         * http-response解码器
         * http服务器端对response编码
         */
        pipeline.addLast("http-encoder", new HttpResponseEncoder());

        /**
         * http-chunked
         * http服务器端支持异步发送大的码流
         */
        pipeline.addLast("http-chunked", new ChunkedWriteHandler());

        /**
         * 压缩
         * Compresses an HttpMessage and an HttpContent in gzip or deflate encoding
         * while respecting the "Accept-Encoding" header.
         * If there is no matching encoding, no compression is done.
         */

        pipeline.addLast("http-process", new HttpBuildHandler());

    }


}
