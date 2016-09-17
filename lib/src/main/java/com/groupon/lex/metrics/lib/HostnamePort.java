/*
 * Copyright (c) 2016, Ariane van der Steldt
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics.lib;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;

/**
 *
 * @author ariane
 */
@Value
public final class HostnamePort {
    private final String hostname;
    private final int port;

    public static HostnamePort valueOf(String param) {
        return valueOf(param, 0);
    }

    public static HostnamePort valueOf(String param, int defaultPort) {
        final String hostname;
        final int port;
        final int colonPos = param.lastIndexOf(':');
        if (colonPos == -1) {
            hostname = param;
            port = defaultPort;
        } else {
            final String prefix = param.substring(0, colonPos);
            final String suffix = param.substring(colonPos + 1);
            boolean parseFailed = false;
            int suffixInt = -1;
            try {
                suffixInt = Integer.parseInt(suffix);
            } catch (NumberFormatException ex) {
                parseFailed = true;
            }
            if (parseFailed) {
                hostname = param;
                port = defaultPort;
            } else {
                hostname = prefix;
                port = suffixInt;
            }
        }
        return new HostnamePort(hostname, port);
    }

    public InetSocketAddress getAddress() {
        return new InetSocketAddress(getHostname(), getPort());
    }

    public List<InetSocketAddress> getAddressList() throws UnknownHostException {
        InetAddress[] addresses = InetAddress.getAllByName(getHostname());
        return Arrays.stream(addresses).map((java.net.InetAddress addr) -> new InetSocketAddress(addr, getPort())).collect(Collectors.toList());
    }
}
