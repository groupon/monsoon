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
package com.groupon.monsoon.remote.history;

import com.groupon.monsoon.remote.history.xdr.iter_result_code;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Represents error codes as returned by iterator manipulation functions.
 * @author ariane
 */
@Getter
@AllArgsConstructor
public enum IteratorErrorCode {
    /** The iterator is not known. */
    UNKNOWN_ITERATOR(iter_result_code.UNKNOWN_ITER);

    /** Error code in its encoded form. */
    private final int encoded;

    /**
     * Change encoded error into IteratorErrorCode.
     * @param encoded The encoded value of the error.
     * @return An instance of IteratorErrorCode mapping to the encoded form.
     * @throws IllegalArgumentException If the encoded form is not an error (success code) or not a valid error.
     */
    public static IteratorErrorCode fromEncodedForm(int encoded) throws IllegalArgumentException {
        if (encoded == iter_result_code.SUCCESS)
            throw new IllegalArgumentException(encoded + " is not an error code (indicates SUCCESS)");

        for (IteratorErrorCode c : values())
            if (c.getEncoded() == encoded) return c;
        throw new IllegalArgumentException("No mapping present for error " + encoded);
    }
}
