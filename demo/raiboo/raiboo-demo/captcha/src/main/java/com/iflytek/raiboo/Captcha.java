package com.iflytek.raiboo;

import com.iflytek.raiboo.captcha.NewCaptcha;
import com.iflytek.raiboo.captcha.Verify;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.sql.SQLException;
import java.util.Map;

/**
 * Root resource (exposed at "myresource" path)
 */
@Path("/captcha")
public class Captcha {

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * curl http://localhost:8080/captcha/new?phonenumber=12312345678\&robot_id=33
     * curl http://localhost:8080/captcha/verify?captcha=YWICES
     *
     * @return String that will be returned as a text/plain response.
     */

    @GET
    @Path("/new")
    @Produces(MediaType.TEXT_PLAIN)
    public String newCaptcha(@QueryParam("phonenumber") String phonenumber, @QueryParam("robot_id") String robot_id) throws SQLException {
        NewCaptcha nc = new NewCaptcha();
        String captcha = nc.genCaptcha(robot_id, phonenumber);
        return captcha;
    }

    @GET
    @Path("/verify")
    @Produces(MediaType.TEXT_PLAIN)
    public String verify(@QueryParam("captcha") String captcha) throws SQLException {
        Verify v = new Verify();
        Map<String, String> result = v.getRobotidPhonenubmer(captcha);
        String validation;
        try{
             validation = v.isValid(result, captcha);
        } catch (SQLException sqle) {
            sqle.printStackTrace();
            validation = "false";
        }
        return validation;
    }

}
