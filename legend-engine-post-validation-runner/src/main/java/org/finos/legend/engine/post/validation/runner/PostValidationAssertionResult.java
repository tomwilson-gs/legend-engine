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

public class PostValidationAssertionResult
{
    private final String assertionId;
    private final String assertionMessage;
    private final boolean assertionPassed;
    private PostValidationAssertionViolations assertionViolations;

    public PostValidationAssertionResult(String assertionId, String assertionMessage, boolean assertionPassed, PostValidationAssertionViolations assertionViolations)
    {
        this.assertionId = assertionId;
        this.assertionMessage = assertionMessage;
        this.assertionPassed = assertionPassed;
        this.assertionViolations = assertionViolations;
    }

    public String getAssertionId()
    {
        return assertionId;
    }

    public String getAssertionMessage()
    {
        return assertionMessage;
    }

    public boolean isAssertionPassed()
    {
        return assertionPassed;
    }

    public PostValidationAssertionViolations getAssertionViolations()
    {
        return assertionViolations;
    }

    public void setAssertionViolations(PostValidationAssertionViolations assertionViolations)
    {
        this.assertionViolations = assertionViolations;
    }
}
