// Copyright 2022 Goldman Sachs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.engine.post.validation.runner;

import java.io.OutputStream;
import java.sql.SQLException;

import org.finos.legend.engine.plan.execution.stores.relational.result.RelationalResult;
import org.finos.legend.engine.plan.execution.stores.relational.serialization.RelationalResultToJsonDefaultSerializer;

import javax.ws.rs.core.StreamingOutput;


public class PostValidationAssertionStreamingOutput implements StreamingOutput
{
    private final String assertionId;
    private final String assertionMessage;
    private final RelationalResult relationalResult;
    private final byte[] b_assertionId = "{\"assertionId\": \"".getBytes();
    private final byte[] b_assertionMessage = "\", \"assertionMessage\": \"".getBytes();
    private final byte[] b_assertionPassed = "\", \"assertionPassed\": ".getBytes();
    private final byte[] b_assertionViolations = ", \"assertionViolations\": {".getBytes();
    private final byte[] b_violations = "\"violations\": ".getBytes();
    private final byte[] b_endResult = "}}".getBytes();

    public PostValidationAssertionStreamingOutput(String assertionId, String assertionMessage, RelationalResult relationalResult)
    {
        this.assertionId = assertionId;
        this.assertionMessage = assertionMessage;
        this.relationalResult = relationalResult;
    }

    @Override
    public void write(OutputStream stream)
    {
        try
        {
            stream.write(b_assertionId);
            stream.write(assertionId.getBytes());
            stream.write(b_assertionMessage);
            stream.write(assertionMessage.getBytes());
            stream.write(b_assertionPassed);

            boolean hasRows = relationalResultHasRows(relationalResult);
            stream.write(String.valueOf(hasRows).getBytes());

            if (hasRows)
            {
                stream.write(b_assertionViolations);
                stream.write(b_violations);

                RelationalResultToJsonDefaultSerializer rowsSerializer = new RelationalResultToJsonDefaultSerializer(relationalResult);
                rowsSerializer.stream(stream);
            }

            stream.write(b_endResult);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            this.relationalResult.close();
        }
    }

    private boolean relationalResultHasRows(RelationalResult result) throws SQLException
    {
        return !result.resultSet.isClosed() && result.resultSet.first();
    }
}

