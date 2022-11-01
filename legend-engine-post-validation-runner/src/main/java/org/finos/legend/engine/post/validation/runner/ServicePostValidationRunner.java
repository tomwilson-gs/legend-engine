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

import org.finos.legend.engine.plan.execution.result.StreamingResult;
import org.finos.legend.engine.plan.execution.result.serialization.SerializationFormat;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_Service;
import org.finos.legend.pure.generated.Root_meta_pure_extension_Extension;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PureSingleExecution;
import org.finos.legend.pure.generated.Root_meta_legend_service_metamodel_PureMultiExecution_Impl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.collections.api.RichIterable;
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
import java.util.HashMap;
import java.util.Map;

public class ServicePostValidationRunner
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ServicePostValidationRunner.class);

    private static final PlanExecutor planExecutor = PlanExecutor.newPlanExecutor(Relational.build(), ServiceStore.build(), InMemory.build());
    private final PureModel pureModel;
    private final Root_meta_legend_service_metamodel_Service pureService;
    private final RichIterable<? extends Root_meta_pure_extension_Extension> extensions;
    private final MutableList<PlanTransformer> transformers;
    private final String pureVersion;
    private final MutableList<CommonProfile> profiles;
    private final SerializationFormat format;

    public ServicePostValidationRunner(PureModel pureModel, Root_meta_legend_service_metamodel_Service pureService,RichIterable<? extends Root_meta_pure_extension_Extension> extensions, MutableList<PlanTransformer> transformers, String pureVersion, MutableList<CommonProfile> profiles, SerializationFormat format)
    {
        this.pureModel = pureModel;
        this.pureService = pureService;
        this.extensions = extensions;
        this.transformers = transformers;
        this.pureVersion = pureVersion;
        this.profiles = profiles;
        this.format = format;
        MetricsHandler.createMetrics(this.getClass());
    }

    public Response runValidationAssertion(String assertionId)
    {
        if (pureService._execution() instanceof Root_meta_legend_service_metamodel_PureMultiExecution_Impl)
        {
            throw new UnsupportedOperationException("MultiExecutions not yet supported");
        }

        Root_meta_legend_service_metamodel_PureSingleExecution singleExecution = (Root_meta_legend_service_metamodel_PureSingleExecution) pureService._execution();
        Mapping mapping = singleExecution._mapping();
        Runtime runtime = singleExecution._runtime();
        LambdaFunction<?> assertion = (LambdaFunction<?>) pureService._postValidations().getFirst()._assertions().getFirst()._assertion();

        return executeValidationAssertion(assertionId, assertion, mapping, runtime);
    }

    private Response executeValidationAssertion(String assertionId, LambdaFunction<?> assertion, Mapping mapping, Runtime runtime)
    {
        SingleExecutionPlan sep = PlanGenerator.generateExecutionPlan(assertion, mapping, runtime, null, this.pureModel, this.pureVersion, PlanPlatform.JAVA, null, this.extensions, this.transformers);

        try
        {
            Map<String, Result> params = new HashMap<>();
            Result result = Subject.doAs(ProfileManagerHelper.extractSubject(profiles), (PrivilegedExceptionAction<Result>) () -> planExecutor.execute(sep, params, null, profiles));

            if (result instanceof StreamingResult)
            {
                return Response.ok(new PostValidationAssertionStreamingOutput(assertionId, "Expected something to be empty", (StreamingResult) result, this.format)).build();
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
}
