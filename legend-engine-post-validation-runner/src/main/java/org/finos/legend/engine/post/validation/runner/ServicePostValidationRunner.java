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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.MutableList;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.result.Result;
import org.finos.legend.engine.plan.execution.stores.inMemory.plugin.InMemory;
import org.finos.legend.engine.plan.execution.stores.relational.plugin.Relational;
import org.finos.legend.engine.plan.execution.stores.relational.result.RelationalResult;
import org.finos.legend.engine.plan.execution.stores.relational.serialization.RelationalResultToCSVSerializer;
import org.finos.legend.engine.plan.execution.stores.service.plugin.ServiceStore;
import org.finos.legend.engine.plan.generation.PlanGenerator;
import org.finos.legend.engine.plan.generation.transformers.PlanTransformer;
import org.finos.legend.engine.plan.platform.PlanPlatform;
import org.finos.legend.engine.protocol.pure.v1.model.executionPlan.SingleExecutionPlan;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.finos.legend.engine.shared.core.kerberos.ProfileManagerHelper;
import org.finos.legend.engine.shared.core.operational.prometheus.MetricsHandler;
import org.finos.legend.pure.generated.*;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping;
import org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.function.LambdaFunction;
import org.finos.legend.pure.m3.coreinstance.meta.pure.runtime.Runtime;
import org.pac4j.core.profile.CommonProfile;

import javax.security.auth.Subject;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

public class ServicePostValidationRunner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServicePostValidationRunner.class);

    private static final PlanExecutor planExecutor = PlanExecutor.newPlanExecutor(Relational.build(), ServiceStore.build(), InMemory.build());
    private final PureModel pureModel;
    private final Root_meta_legend_service_metamodel_Service pureService;
    private final ObjectMapper objectMapper;
    private final PlanExecutor executor;
    private final RichIterable<? extends Root_meta_pure_extension_Extension> extensions;
    private final MutableList<PlanTransformer> transformers;
    private final String pureVersion;
    private final MutableList<CommonProfile> profiles;
    private final String metricsContext;

    public ServicePostValidationRunner(PureModel pureModel, Root_meta_legend_service_metamodel_Service pureService, ObjectMapper objectMapper, PlanExecutor executor, RichIterable<? extends Root_meta_pure_extension_Extension> extensions, MutableList<PlanTransformer> transformers, String pureVersion, MutableList<CommonProfile> profiles, String metricsContext)
    {
        this.pureModel = pureModel;
        this.pureService = pureService;
        this.objectMapper = (objectMapper == null) ? ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports() : objectMapper;
        this.executor = executor;
        this.extensions = extensions;
        this.transformers = transformers;
        this.pureVersion = pureVersion;
        this.profiles = profiles;
        MetricsHandler.createMetrics(this.getClass());
        this.metricsContext = metricsContext;
    }

    public boolean runValidationAssertion(String assertionId)
    {
        if (pureService._execution() instanceof Root_meta_legend_service_metamodel_PureMultiExecution_Impl)
        {
            throw new UnsupportedOperationException("MultiExecutions not yet supported");
        }

        Root_meta_legend_service_metamodel_PureSingleExecution singleExecution = (Root_meta_legend_service_metamodel_PureSingleExecution) pureService._execution();
        Mapping mapping = singleExecution._mapping();
        Runtime runtime = singleExecution._runtime();
        LambdaFunction<?> assertion = (LambdaFunction<?>) pureService._postValidations().getFirst()._assertions().getFirst()._assertion();

        return executeValidationAssertion(assertion, mapping, runtime);
    }

    private boolean executeValidationAssertion(LambdaFunction<?> assertion, Mapping mapping, Runtime runtime)
    {
        SingleExecutionPlan sep = PlanGenerator.generateExecutionPlan(assertion, mapping, runtime, null, this.pureModel, this.pureVersion, PlanPlatform.JAVA, null, this.extensions, this.transformers);

        try
        {
            Map<String, Result> params = new HashMap<>();
            Result result = Subject.doAs(ProfileManagerHelper.extractSubject(profiles), (PrivilegedExceptionAction<Result>) () -> planExecutor.execute(sep, params, "", profiles));

            System.out.println(result);

            if (result instanceof RelationalResult)
            {
                RelationalResult relationalResult = (RelationalResult) result;
                RelationalResultToCSVSerializer serializer = new RelationalResultToCSVSerializer(relationalResult);
                String output = serializer.flush().toString();
                System.out.println(output);
            }
        }
        catch (PrivilegedActionException e)
        {
            throw new RuntimeException(e);
        }

        return false;
    }
}
