/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author ariane
 */
public interface Path {
    public List<String> getPath();

    public default StringBuilder configString() { return new StringBuilder(getPathString()); }

    public default String getPathString() {
        return String.join(".",
                getPath().stream()
                        .map(ConfigSupport::maybeQuoteIdentifier)
                        .map(StringBuilder::toString)
                        .collect(Collectors.toList()));
    }
}
