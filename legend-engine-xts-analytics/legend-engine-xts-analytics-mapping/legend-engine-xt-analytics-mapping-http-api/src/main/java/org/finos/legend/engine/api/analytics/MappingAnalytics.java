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

package org.finos.legend.engine.api.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentracing.Scope;
import io.opentracing.util.GlobalTracer;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.utility.ListIterate;
import org.finos.legend.engine.api.analytics.model.MappingModelCoverageAnalysisInput;
import org.finos.legend.engine.api.analytics.model.MappingRuntimeCompatibilityAnalysisInput;
import org.finos.legend.engine.api.analytics.model.MappingRuntimeCompatibilityAnalysisResult;
import org.finos.legend.engine.integration.analytics.MappingModelCoverageAnalysis;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.HelperModelBuilder;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.HelperRuntimeBuilder;
import org.finos.legend.engine.language.pure.compiler.toPureGraph.PureModel;
import org.finos.legend.engine.language.pure.modelManager.ModelManager;
import org.finos.legend.engine.protocol.analytics.model.MappingModelCoverageAnalysisResult;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.runtime.PackageableRuntime;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.finos.legend.engine.shared.core.api.result.ManageConstantResult;
import org.finos.legend.engine.shared.core.identity.Identity;
import org.finos.legend.engine.shared.core.kerberos.ProfileManagerHelper;
import org.finos.legend.engine.shared.core.operational.errorManagement.ExceptionTool;
import org.finos.legend.engine.shared.core.operational.http.InflateInterceptor;
import org.finos.legend.engine.shared.core.operational.logs.LoggingEventType;
import org.finos.legend.pure.m3.coreinstance.meta.pure.mapping.Mapping;
import org.pac4j.core.profile.CommonProfile;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.jax.rs.annotations.Pac4JProfileManager;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Api(tags = "Analytics - Model")
@Path("pure/v1/analytics/mapping")
public class MappingAnalytics
{
    private final ObjectMapper objectMapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();
    private final ModelManager modelManager;

    public MappingAnalytics(ModelManager modelManager)
    {
        this.modelManager = modelManager;
    }

    @POST
    @Path("modelCoverage")
    @ApiOperation(value = "Analyze the mapping to generate information about mapped classes and mapped properties of each class")
    @Consumes({MediaType.APPLICATION_JSON, InflateInterceptor.APPLICATION_ZLIB})
    @Produces(MediaType.APPLICATION_JSON)
    public Response analyzeModelCoverage(MappingModelCoverageAnalysisInput input,
                                         @QueryParam("returnMappedEntityInfo") @DefaultValue("false") boolean returnMappedEntityInfo,
                                         @QueryParam("returnMappedPropertyInfo") @DefaultValue("false") boolean returnMappedPropertyInfo,
                                         @QueryParam("returnLightGraph") @DefaultValue("false") boolean returnLightGraph,
                                         @ApiParam(hidden = true) @Pac4JProfileManager ProfileManager<CommonProfile> pm)
    {
        MutableList<CommonProfile> profiles = ProfileManagerHelper.extractProfiles(pm);
        Identity identity = Identity.makeIdentity(profiles);
        PureModel pureModel = this.modelManager.loadModel(input.model, input.clientVersion, identity, null);
        PureModelContextData pureModelContextData = this.modelManager.loadData(input.model, input.clientVersion, identity);
        Mapping mapping = input.mapping == null ? null : pureModel.getMapping(input.mapping);
        try (Scope scope = GlobalTracer.get().buildSpan("Mapping: analysis").startActive(true))
        {
            try
            {
                MappingModelCoverageAnalysisResult result = MappingModelCoverageAnalysis.analyze(mapping, pureModel, pureModelContextData, input.clientVersion, this.objectMapper, returnMappedEntityInfo, returnMappedPropertyInfo, returnLightGraph);
                return ManageConstantResult.manageResult(identity.getName(), result);
            }
            catch (Exception e)
            {
                return ExceptionTool.exceptionManager(e, LoggingEventType.ANALYTICS_ERROR, Response.Status.BAD_REQUEST, identity.getName());
            }
        }
    }

    @POST
    @Path("runtimeCompatibility")
    @ApiOperation(value = "Analyze the mapping to identify compatible runtimes")
    @Consumes({MediaType.APPLICATION_JSON, InflateInterceptor.APPLICATION_ZLIB})
    @Produces(MediaType.APPLICATION_JSON)
    public Response analyzeMappingRuntimeCompatibility(MappingRuntimeCompatibilityAnalysisInput input,
                                                       @ApiParam(hidden = true) @Pac4JProfileManager ProfileManager<CommonProfile> pm)
    {
        MutableList<CommonProfile> profiles = ProfileManagerHelper.extractProfiles(pm);
        Identity identity = Identity.makeIdentity(profiles);
        PureModelContextData pureModelContextData = this.modelManager.loadData(input.model, input.clientVersion, identity);
        PureModel pureModel = this.modelManager.loadModel(pureModelContextData, input.clientVersion, identity, null);
        Mapping mapping = input.mapping == null ? null : pureModel.getMapping(input.mapping);
        try (Scope scope = GlobalTracer.get().buildSpan("Mapping: analysis").startActive(true))
        {
            try
            {
                return ManageConstantResult.manageResult(identity.getName(), new MappingRuntimeCompatibilityAnalysisResult(
                        ListIterate.collect(HelperRuntimeBuilder.getMappingCompatibleRuntimes(
                                mapping,
                                ListIterate.selectInstancesOf(pureModelContextData.getElements(), PackageableRuntime.class),
                                pureModel), runtime -> HelperModelBuilder.getElementFullPath(runtime, pureModel.getExecutionSupport()))));
            }
            catch (Exception e)
            {
                return ExceptionTool.exceptionManager(e, LoggingEventType.ANALYTICS_ERROR, Response.Status.BAD_REQUEST, identity.getName());
            }
        }
    }
}
