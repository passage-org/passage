package org.apache.jena.riot.rowset.rw.rs_json;

// Disclaimer: this is a slight modified version of Apache Jena's
// RowSetJSONStreaming that reads a `metadata` field when it
// exists.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.jena.atlas.data.BagFactory;
import org.apache.jena.atlas.data.DataBag;
import org.apache.jena.atlas.data.ThresholdPolicy;
import org.apache.jena.atlas.data.ThresholdPolicyFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.jena.riot.rowset.RowSetReader;
import org.apache.jena.riot.rowset.RowSetReaderRegistry;
import org.apache.jena.riot.rowset.rw.RowSetReaderJSONStreaming;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.SyntaxLabels;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.exec.QueryExecResult;
import org.apache.jena.sparql.resultset.ResultSetException;
import org.apache.jena.sparql.system.SerializationFactoryFinder;
import org.apache.jena.sparql.util.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Objects;
import java.util.function.Supplier;

public class RowSetReaderJSONWithMetadata implements RowSetReader {

    public static void install() {
        RowSetReaderRegistry.register(ResultSetLang.RS_JSON,
                (lang) -> {
                    if (!Objects.equals(lang, ResultSetLang.RS_JSON))
                        throw new ResultSetException("RowSet for JSON asked for a " + lang);
                    return new RowSetReaderJSONWithMetadata();
                });
    }

    /* ************************************************************************ */

    private static final Logger log = LoggerFactory.getLogger(RowSetReaderJSONStreaming.class);

    @Override
    public QueryExecResult readAny(InputStream in, Context context) {
        return process(in,context);
    }

    public static QueryExecResult process(InputStream in, Context context) {
        context = context == null ? ARQ.getContext() : context;

        QueryExecResult result = null;
        RowSetBuffered<RowSetJSONStreamingWithMetadata<?>> rs = createRowSet(in, context);

        Boolean searchHeaderEagerly = context.get(RowSetReaderJSONStreaming.rsJsonSearchHeadEagerly, false);
        if (Boolean.TRUE.equals(searchHeaderEagerly)) {
            // This triggers searching for the first header
            rs.getResultVars();
        }

        // If there are no bindings we check for an ask result
        if (!rs.hasNext()) {
            // Unwrapping in order to access the ask result
            RowSetJSONStreamingWithMetadata<?> inner = rs.getDelegate();
            Boolean askResult = inner.getAskResult();

            if (askResult != null) {
                result = new QueryExecResult(askResult);
            }
        }

        if (result == null) {
            result = new QueryExecResult(rs);
        }

        return result;
    }

    public static RowSetBuffered<RowSetJSONStreamingWithMetadata<?>> createRowSet(InputStream in, Context context) {
        // Extra cxt variable needed because of lambda below
        Context cxt = context == null ? ARQ.getContext() : context;

        boolean inputGraphBNodeLabels = cxt.isTrue(ARQ.inputGraphBNodeLabels);
        LabelToNode labelMap = inputGraphBNodeLabels
                ? SyntaxLabels.createLabelToNodeAsGiven()
                : SyntaxLabels.createLabelToNode();

        Supplier<DataBag<Binding>> bufferFactory = () -> {
            ThresholdPolicy<Binding> policy = ThresholdPolicyFactory.policyFromContext(cxt);
            DataBag<Binding> r = BagFactory.newDefaultBag(policy, SerializationFactoryFinder.bindingSerializationFactory());
            return r;
        };

        ValidationSettings validationSettings = RowSetReaderJSONStreaming.configureValidationFromContext(new ValidationSettings(), cxt);

        // Log warnings but otherwise raise exceptions without logging
        return RowSetJSONStreamingWithMetadata.createBuffered(in, labelMap, bufferFactory, validationSettings,
                ErrorHandlerFactory.errorHandlerWarnOrExceptions(log));
    }
}
