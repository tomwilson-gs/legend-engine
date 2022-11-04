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

import org.finos.legend.engine.plan.execution.result.StreamingResult;
import org.finos.legend.engine.plan.execution.result.object.StreamingObjectResult;
import org.finos.legend.engine.plan.execution.result.object.StreamingObjectResultJSONSerializer;
import org.finos.legend.engine.plan.execution.result.serialization.SerializationFormat;
import org.finos.legend.engine.plan.execution.stores.relational.result.RelationalResult;
import org.finos.legend.engine.plan.execution.stores.relational.serialization.RelationalResultToPureTDSSerializer;
import org.finos.legend.engine.plan.execution.stores.relational.serialization.RelationalResultToPureTDSToObjectSerializer;
import org.finos.legend.engine.plan.execution.stores.relational.serialization.RelationalResultToJsonDefaultSerializer;

import javax.ws.rs.core.StreamingOutput;


public class PostValidationAssertionStreamingOutput implements StreamingOutput
{
    private final String assertionId;
    private final String assertionMessage;
    private final StreamingResult result;
    private final SerializationFormat format;
    private final byte[] b_assertionId = "{\"assertionId\": \"".getBytes();
    private final byte[] b_assertionMessage = "\", \"assertionMessage\": \"".getBytes();
    private final byte[] b_assertionResult = "\", \"assertionResult\": ".getBytes();
    private final byte[] b_assertionViolations = ", \"assertionViolations\":".getBytes();
    private final byte[] b_endResult = "}".getBytes();

    public PostValidationAssertionStreamingOutput(String assertionId, String assertionMessage, StreamingResult result, SerializationFormat format)
    {
        this.assertionId = assertionId;
        this.assertionMessage = assertionMessage;
        this.result = result;
        this.format = format;
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
            stream.write(b_assertionResult);

            PostValidationAssertionResult validationResult = getValidationResult();
            stream.write(("\"" + validationResult + "\"").getBytes());

            if (PostValidationAssertionResult.FAILED.equals(validationResult))
            {
                stream.write(b_assertionViolations);

                if (result instanceof RelationalResult)
                {
                    switch (this.format)
                    {
                        case PURE_TDSOBJECT:
                            new RelationalResultToPureTDSToObjectSerializer((RelationalResult) result).stream(stream);
                            break;
                        case PURE:
                            new RelationalResultToPureTDSSerializer((RelationalResult) result).stream(stream);
                            break;
                        case DEFAULT:
                            new RelationalResultToJsonDefaultSerializer((RelationalResult) result).stream(stream);
                            break;
                        default:
                            throw new RuntimeException(this.format + " format not currently supported for RelationalResult");
                    }
                }
                else if (result instanceof StreamingObjectResult)
                {
                    switch (this.format)
                    {
                        case DEFAULT:
                            new StreamingObjectResultJSONSerializer((StreamingObjectResult<?>) result).stream(stream);
                            break;
                        default:
                            throw new RuntimeException(this.format + " format not currently supported for StreamingObjectResult");
                    }
                }
            }

            stream.write(b_endResult);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            this.result.close();
        }
    }

    private boolean resultHasRows(StreamingResult result) throws SQLException
    {
        if (result instanceof RelationalResult)
        {
            return !((RelationalResult) result).resultSet.isClosed() && ((RelationalResult) result).resultSet.isBeforeFirst();
        }
        else if (result instanceof StreamingObjectResult)
        {
            return !((RelationalResult) ((StreamingObjectResult<?>) result).getChildResult()).resultSet.isClosed() && ((RelationalResult) ((StreamingObjectResult<?>) result).getChildResult()).resultSet.isBeforeFirst();
        }
        else
        {
            throw new RuntimeException(result.getClass() + " not currently supported for Post Validation execution");
        }
    }

    private PostValidationAssertionResult getValidationResult()
    {
        try
        {
            boolean hasRows = resultHasRows(this.result);
            return hasRows ? PostValidationAssertionResult.FAILED : PostValidationAssertionResult.PASSED;
        }
        catch (SQLException e)
        {
            return PostValidationAssertionResult.ERROR;
        }
    }
}

