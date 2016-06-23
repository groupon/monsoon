/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.api.endpoints;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonWriter;
import com.groupon.lex.metrics.config.ConfigurationException;
import com.groupon.lex.metrics.config.ParserSupport;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author ariane
 */
public class ExprValidate extends HttpServlet {
    private static final Gson gson_ = new GsonBuilder().setPrettyPrinting().create();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Optional<String> expr = Optional.ofNullable(req.getParameter("expr"));

        if (!expr.isPresent()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "expression must be provided");
            return;
        }

        final ResponseObj ro = new ResponseObj();
        ro.ok = true;
        try {
            ro.normalizedQuery = new ParserSupport(expr.get()).expression().configString().toString();
        } catch (ConfigurationException ce) {
            ro.ok = false;
            ro.parseErrors = ce.getParseErrors();
        }

        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");
        gson_.toJson(ro, ResponseObj.class, new JsonWriter(resp.getWriter()));
    }

    private static class ResponseObj {
        @SerializedName("ok")
        public boolean ok;
        @SerializedName("parse_errors")
        public List<String> parseErrors = null;
        @SerializedName("normalized_query")
        public String normalizedQuery = null;
    }
}
