package dmp;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by taochen4 on 2017/2/17.
 */
public class HttpBuildHandler extends ChannelHandlerAdapter {

    private static Logger logger = Logger.getLogger(HttpBuildHandler.class);
    private FullHttpRequest _request = null;
    static List<Map<String, String>> dbresult;
    private static Gson gson = new Gson();
    private FlowAnalysis flowAnalysis = new FlowAnalysis();

    public static class Result {
        public String ret;
        public String errorMsg;

        public Integer enter_max;
        public Integer enter_min;
        public Integer exhibition_max;
        public Integer exhibition_min;
        public Integer play_max;
        public Integer play_min;
        public List<FlowAnalysis.StatisticRes> resList;

        public Result() {
            ret = "0";
            errorMsg = "";
            enter_max = 0;
            enter_min = Integer.MAX_VALUE;
            exhibition_max = 0;
            exhibition_min = Integer.MAX_VALUE;
            play_max = 0;
            play_min = Integer.MAX_VALUE;
        }
    }

    public HttpBuildHandler() throws SQLException, ParseException {
        flowAnalysis.init();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelActive();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String[] url = new String[0];
        String rtn;
        Map<String, String> data = new HashMap<>();
        Result result = new Result();

        if (msg instanceof FullHttpRequest) {
            _request = (FullHttpRequest) msg;
//            curl  -d '{"token":"7Tks_1395749149_9W4Q", "ver":"1.0"}' http://localhost:8080/apis/app.php?func=taglist
            if (_request.method() == HttpMethod.GET && _request.decoderResult().isSuccess()) {
//                System.out.println("Valid http request");
                logger.info("New http request is valid");
            } else {
                logger.warn("Http request method is not post");
                sendError(ctx, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        } else {
            logger.warn("http request is not valid");
            sendError(ctx, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        try {
            Type collectionType = new TypeToken<Map<String, String>>() {
            }.getType();
            if (_request.content().toString(CharsetUtil.UTF_8).length() > 0) {
                data = gson.fromJson(_request.content().toString(CharsetUtil.UTF_8), collectionType);
            }
            url = ((FullHttpRequest) msg).uri().split("/");
//            System.out.println(((FullHttpRequest) msg).uri().toString());
        } catch (Exception e) {
            logger.warn("parse http request exception");
            e.printStackTrace();
        } finally {
            if (url.length == 3 && url[1].equals("raiboo") && url[2].equals("id_analysis")) {
                List<FlowAnalysis.StatisticRes> statisticResList = flowAnalysis.getStatisticResList(new Date().getTime()/1000/60*60);
                result.resList = statisticResList;
                for (FlowAnalysis.StatisticRes sr : statisticResList) {
                    if (sr.enter_count > result.enter_max) {
                        result.enter_max = sr.enter_count;
                    }
                    if (sr.enter_count < result.enter_min) {
                        result.enter_min = sr.enter_count;
                    }
                    if (sr.exhibition_count > result.exhibition_max) {
                        result.exhibition_max = sr.exhibition_count;
                    }
                    if (sr.exhibition_count < result.exhibition_min) {
                        result.exhibition_min = sr.exhibition_count;
                    }
                    if (sr.play_count > result.play_max) {
                        result.play_max = sr.play_count;
                    }
                    if (sr.play_count < result.play_min) {
                        result.play_min = sr.play_count;
                    }
                }
            } else { // url路径长度错误
                result.ret = "20010";
                result.errorMsg = "请求错误，请检查请求格式";
            }
            rtn = gson.toJson(result);
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK, Unpooled.wrappedBuffer(rtn.getBytes("UTF-8")));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        ctx.write(response);
        ctx.flush();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
//        logger.info("HttpServerInboundHandler.channelReadComplete");
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
//        super.exceptionCaught(ctx, cause);
    }

    private static void sendError(ChannelHandlerContext ctx, HttpResponseStatus status) {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status,
                Unpooled.copiedBuffer("Failure:     " + status.toString() + "\r\n", CharsetUtil.UTF_8));
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
    }
}
