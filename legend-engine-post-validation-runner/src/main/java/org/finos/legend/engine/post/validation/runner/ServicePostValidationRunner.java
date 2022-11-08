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
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PostValidation;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PostValidationAssertion;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PureMultiExecution_Impl;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PureSingleExecution;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_Service;
import org.finos.legend.pure.generated.Root_meta_pure_extension_Extension;
import org.finos.legend.pure.generated.Root_meta_pure_tds_TabularDataSet;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.FunctionDefinition;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.InstanceValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import static org.finos.legend.pure.generated.core_legend_service_validation.Root_meta_legend_service_validation_executeAssertion_TabularDataSet_1__FunctionDefinition_1__TabularDataSet_1_;
import static org.finos.legend.pure.generated.core_legend_service_validation.Root_meta_legend_service_validation_extractAssertMessage_FunctionDefinition_1__String_1_;
import static org.finos.legend.pure.generated.core_legend_service_validation.Root_meta_legend_service_validation_generateValidationQuery_FunctionDefinition_1__FunctionDefinition_1__FunctionDefinition_1_;

public class ServicePostValidationRunner
{
    private static final PlanExecutor planExecutor = PlanExecutor.newPlanExecutor(Relational.build(), ServiceStore.build(), InMemory.build());
    private final PureModel pureModel;
    private final Root_meta_legend_service_metamodel_Service pureService;
    private final List<Variable> rawParams;
    private final Mapping mapping;
    private final Runtime runtime;
    private final RichIterable<? extends Root_meta_pure_extension_Extension> extensions;
    private final MutableList<PlanTransformer> transformers;
    private final String pureVersion;
    private final MutableList<CommonProfile> profiles;
    private final SerializationFormat format;

    public ServicePostValidationRunner(PureModel pureModel, Root_meta_legend_service_metamodel_Service pureService, List<Variable> rawParams, RichIterable<? extends Root_meta_pure_extension_Extension> extensions, MutableList<PlanTransformer> transformers, String pureVersion, MutableList<CommonProfile> profiles, SerializationFormat format)
    {
        if (pureService._execution() instanceof Root_meta_legend_service_metamodel_PureMultiExecution_Impl)
        {
            throw new UnsupportedOperationException("MultiExecutions not yet supported");
        }
        Root_meta_legend_service_metamodel_PureSingleExecution singleExecution = (Root_meta_legend_service_metamodel_PureSingleExecution) pureService._execution();

        this.mapping = singleExecution._mapping();
        this.runtime = singleExecution._runtime();
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

        return executeValidationAssertion(assertionId, paramsWithAssertion);
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

        LambdaFunction<?> queryFunc = (LambdaFunction<?>) ((Root_meta_legend_service_metamodel_PureSingleExecution) pureService._execution())._func();
        FunctionDefinition<?> assertQuery = Root_meta_legend_service_validation_generateValidationQuery_FunctionDefinition_1__FunctionDefinition_1__FunctionDefinition_1_(queryFunc, assertion, pureModel.getExecutionSupport());
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

//    private Response executeValidationAssertionInMemory(String assertionId, Pair<RichIterable<?>, LambdaFunction<?>> paramsWithAssertion)
//    {
//        RichIterable<?> params = paramsWithAssertion.getOne();
//        LambdaFunction<?> assertion = paramsWithAssertion.getTwo();
//
//        LambdaFunction<?> queryFunc = (LambdaFunction<?>) ((Root_meta_legend_service_metamodel_PureSingleExecution) pureService._execution())._func();
//        SingleExecutionPlan sep = PlanGenerator.generateExecutionPlan(queryFunc, this.mapping, this.runtime,  null, this.pureModel, this.pureVersion, PlanPlatform.JAVA, null, this.extensions, this.transformers);
//        String assertMessage = Root_meta_legend_service_validation_extractAssertMessage_FunctionDefinition_1__String_1_(assertion, pureModel.getExecutionSupport());
//
//        try
//        {
//            Result queryResult = executePlan(sep, new HashMap<>());
//            org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Result<Object> pureResult = queryResult.accept(new ResultToPureResultVisitor());
//
//            Root_meta_pure_tds_TabularDataSet tdsResult = Root_meta_legend_service_validation_executeAssertion_TabularDataSet_1__FunctionDefinition_1__TabularDataSet_1_((Root_meta_pure_tds_TabularDataSet) pureResult._values().getAny(), assertion, pureModel.getExecutionSupport());
//
//            if (queryResult instanceof StreamingResult)
//            {
//                return Response.ok(new PostValidationAssertionStreamingOutput(assertionId, assertMessage, (StreamingResult) queryResult, this.format)).build();
//            }
//            else
//            {
//                return Response.serverError().build();
//            }
//        }
//        catch (PrivilegedActionException e)
//        {
//            throw new RuntimeException(e);
//        }
//    }

    private Result executePlan(SingleExecutionPlan plan, Map<String, Result> params) throws PrivilegedActionException
    {
        return Subject.doAs(ProfileManagerHelper.extractSubject(profiles), (PrivilegedExceptionAction<Result>) () -> planExecutor.execute(plan, params, null, profiles));
    }
}
