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

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.plan.execution.result.ConstantResult;
import org.finos.legend.engine.plan.execution.result.StreamingResult;
import org.finos.legend.engine.plan.execution.result.serialization.SerializationFormat;
import org.finos.legend.engine.plan.execution.stores.relational.result.RelationalResult;
import org.finos.legend.engine.protocol.pure.v1.model.valueSpecification.Variable;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_KeyedExecutionParameter;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PostValidation;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PostValidationAssertion;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PureMultiExecution;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PureMultiExecution_Impl;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PureSingleExecution;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PureSingleExecution_Impl;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_Service;
import org.finos.legend.pure.generated.Root_meta_pure_extension_Extension;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.FunctionDefinition;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.InstanceValue;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.execution.stores.inMemory.plugin.InMemory;
import org.finos.legend.engine.plan.execution.stores.relational.plugin.Relational;
import org.finos.legend.engine.plan.execution.stores.service.plugin.ServiceStore;
import org.finos.legend.engine.plan.generation.PlanGenerator;
import org.finos.legend.engine.plan.generation.transformers.PlanTransformer;
import org.finos.legend.engine.plan.platform.PlanPlatform;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.SingleExecutionPlan;
import org.finos.legend.engine.shared.core.kerberos.ProfileManagerHelper;
import org.finos.legend.engine.shared.core.operational.prometheus.MetricsHandler;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction;
import org.finos.legend.pure.m3.coreinstance.meta.pure.runtime.Runtime;
import org.pac4j.core.profile.CommonProfile;

import javax.security.auth.Subject;
import javax.ws.rs.core.Response;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.finos.legend.pure.generated.core_legend_service_validation.Root_meta_legend_service_validation_extractAssertMessage_FunctionDefinition_1__String_1_;
import static org.finos.legend.pure.generated.core_legend_service_validation.Root_meta_legend_service_validation_generateValidationQuery_FunctionDefinition_1__FunctionDefinition_1__FunctionDefinition_1_;

public class ServicePostValidationRunner
{
    private static final PlanExecutor planExecutor = PlanExecutor.newPlanExecutor(Relational.build(), ServiceStore.build(), InMemory.build());
    private final PureModel pureModel;
    private final Root_meta_legend_service_metamodel_Service pureService;
    private final List<Variable> rawParams;
    private final RichIterable<? extends Root_meta_pure_extension_Extension> extensions;
    private final MutableList<PlanTransformer> transformers;
    private final String pureVersion;
    private final MutableList<CommonProfile> profiles;
    private final SerializationFormat format;
    private LambdaFunction<?> queryFunc;
    private Mapping mapping;
    private Runtime runtime;

    public ServicePostValidationRunner(PureModel pureModel, Root_meta_legend_service_metamodel_Service pureService, List<Variable> rawParams, RichIterable<? extends Root_meta_pure_extension_Extension> extensions, MutableList<PlanTransformer> transformers, String pureVersion, MutableList<CommonProfile> profiles, SerializationFormat format)
    {
        this.pureModel = pureModel;
        this.pureService = pureService;
        this.rawParams = rawParams;
        this.extensions = extensions;
        this.transformers = transformers;
        this.pureVersion = pureVersion;
        this.profiles = profiles;
        this.format = format;
        MetricsHandler.createMetrics(this.getClass());
    }

    public Response runValidationAssertion(String assertionId)
    {
        Pair<RichIterable<?>, LambdaFunction<?>> paramsWithAssertion = findParamsWithAssertion(assertionId);
        instantiateQueryMappingAndRuntime(paramsWithAssertion);

        return executeValidationAssertion(assertionId, paramsWithAssertion);
    }

    private void instantiateQueryMappingAndRuntime(Pair<RichIterable<?>, LambdaFunction<?>> paramsWithAssertion)
    {
        if (pureService._execution() instanceof Root_meta_legend_service_metamodel_PureSingleExecution_Impl)
        {
            Root_meta_legend_service_metamodel_PureSingleExecution singleExecution = (Root_meta_legend_service_metamodel_PureSingleExecution) pureService._execution();
            this.queryFunc = (LambdaFunction<?>) singleExecution._func();
            this.mapping = singleExecution._mapping();
            this.runtime = singleExecution._runtime();
        }
        else if (pureService._execution() instanceof Root_meta_legend_service_metamodel_PureMultiExecution_Impl)
        {
            Root_meta_legend_service_metamodel_PureMultiExecution multiExecution = (Root_meta_legend_service_metamodel_PureMultiExecution) pureService._execution();
            // Find index in service path of execution key param
            int keyIndex = findExecutionKeyIndex(multiExecution);

            // Find value of execution key param
            RichIterable<?> params = paramsWithAssertion.getOne();
            Object rawParam = params.toList().get(keyIndex);
            String executionParamValue = (String) ((InstanceValue) ((LambdaFunction<?>) rawParam)._expressionSequence().getAny())._values().getAny();

            // Find execution that matches the param, then extract and instantiate query/mapping/runtime from execution
            for (Root_meta_legend_service_metamodel_KeyedExecutionParameter keyedParam : multiExecution._executionParameters())
            {
                if (keyedParam._key().equals(executionParamValue))
                {
                    this.queryFunc = (LambdaFunction<?>) multiExecution._func();
                    this.mapping = keyedParam._mapping();
                    this.runtime = keyedParam._runtime();
                }
            }

            // Throw exception if no execution matches the param
            if (this.mapping == null)
            {
                throw new NoSuchElementException("No execution parameter with key '" + executionParamValue + "'");
            }
        }
        else
        {
            throw new UnsupportedOperationException("Execution type unsupported");
        }
    }

    private int findExecutionKeyIndex(Root_meta_legend_service_metamodel_PureMultiExecution multiExecution)
    {
        String servicePattern = this.pureService._pattern();
        Matcher m = Pattern.compile("\\{(\\w*)\\}").matcher(servicePattern);
        List<String> paramGroups = FastList.newList();
        while (m.find())
        {
            paramGroups.add(m.group(1));
        }
        int keyIndex = paramGroups.indexOf(multiExecution._executionKey());
        if (keyIndex == -1)
        {
            throw new NoSuchElementException("No param matching key found in service pattern");
        }

        return keyIndex;
    }

    private Pair<RichIterable<?>, LambdaFunction<?>> findParamsWithAssertion(String assertionId)
    {
        for (Root_meta_legend_service_metamodel_PostValidation<?> postValidation : pureService._postValidations())
        {
            for (Root_meta_legend_service_metamodel_PostValidationAssertion<?> assertion : postValidation._assertions())
            {
                if (assertion._id().equals(assertionId))
                {
                    return Tuples.pair(postValidation._parameters(), (LambdaFunction<?>) assertion._assertion());
                }
            }
        }

        throw new NoSuchElementException("Assertion " + assertionId + " not found");
    }

    private MutableMap<String, Result> evaluateParameters(RichIterable<?> parameters)
    {
        List<Result> evaluatedParams = FastList.newList();

        for (Object parameter : parameters)
        {
            if (parameter instanceof LambdaFunction<?>)
            {
                Object innerParam = ((LambdaFunction<?>) parameter)._expressionSequence().getAny();

                if (innerParam instanceof InstanceValue)
                {
                    evaluatedParams.add(new ConstantResult(((InstanceValue) innerParam)._values().getAny()));
                }
                else
                {
                    SingleExecutionPlan sep = PlanGenerator.generateExecutionPlan((LambdaFunction<?>) parameter, this.mapping, this.runtime, null, this.pureModel, this.pureVersion, PlanPlatform.JAVA, null, this.extensions, this.transformers);

                    try
                    {
                        Result paramResult = executePlan(sep, new HashMap<>());

                        if (paramResult instanceof RelationalResult)
                        {
                            ResultSet resultSet = ((RelationalResult) paramResult).resultSet;
                            resultSet.next();
                            evaluatedParams.add(new ConstantResult(resultSet.getObject(1)));
                        }
                        else
                        {
                            evaluatedParams.add(paramResult);
                        }
                    }
                    catch (PrivilegedActionException | SQLException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
            else
            {
                throw new UnsupportedOperationException("Not supported");
            }
        }

        return ListIterate.zip(this.rawParams, evaluatedParams).toMap(p -> p.getOne().name, Pair::getTwo);
    }

    private Response executeValidationAssertion(String assertionId, Pair<RichIterable<?>, LambdaFunction<?>> paramsWithAssertion)
    {
        RichIterable<?> params = paramsWithAssertion.getOne();
        LambdaFunction<?> assertion = paramsWithAssertion.getTwo();

        FunctionDefinition<?> assertQuery = Root_meta_legend_service_validation_generateValidationQuery_FunctionDefinition_1__FunctionDefinition_1__FunctionDefinition_1_(this.queryFunc, assertion, pureModel.getExecutionSupport());
        String assertMessage = Root_meta_legend_service_validation_extractAssertMessage_FunctionDefinition_1__String_1_(assertion, pureModel.getExecutionSupport());

        MutableMap<String, Result> evaluatedParams = evaluateParameters(params);

        SingleExecutionPlan sep = PlanGenerator.generateExecutionPlan((LambdaFunction<?>) assertQuery, this.mapping, this.runtime,  null, this.pureModel, this.pureVersion, PlanPlatform.JAVA, null, this.extensions, this.transformers);

        try
        {
            Result queryResult = executePlan(sep, evaluatedParams);

            if (queryResult instanceof StreamingResult)
            {
                return Response.ok(new PostValidationAssertionStreamingOutput(assertionId, assertMessage, (StreamingResult) queryResult, this.format)).build();
            }
            else
            {
                return Response.serverError().build();
            }
        }
        catch (PrivilegedActionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private Result executePlan(SingleExecutionPlan plan, Map<String, Result> params) throws PrivilegedActionException
    {
        return Subject.doAs(ProfileManagerHelper.extractSubject(profiles), (PrivilegedExceptionAction<Result>) () -> planExecutor.execute(plan, params, null, profiles));
    }
}
