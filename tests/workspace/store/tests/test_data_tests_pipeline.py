from pathlib import Path
import os
import pytest
from data_factory_testing_framework import TestFramework, TestFrameworkType
from data_factory_testing_framework.models import Pipeline
from data_factory_testing_framework.state import PipelineRunState, RunParameter, RunParameterType


pytestmark = pytest.mark.fabric_pipeline_test

@pytest.fixture
def test_framework(request: pytest.FixtureRequest) -> TestFramework:
    # Project root as seen by pytest
    project_root = Path(request.config.rootpath)

    # Your Fabric workspace folder
    workspace_root = project_root / "workspace"

    return TestFramework(
        framework_type=TestFrameworkType.Fabric,
        root_folder_path=str(workspace_root),
    )


@pytest.fixture
def pipeline(test_framework: TestFramework) -> Pipeline:
    return test_framework.get_pipeline_by_name("tests_orchestration_pipeline")


@pytest.mark.parametrize("env", ["DEV", "TEST", "PROD"])
def test_env_parameter_flows(pipeline: Pipeline, env):
    state = PipelineRunState(
        parameters=[
            RunParameter(
                RunParameterType.Pipeline,
                name="EnvironmentName",
                value=env,
            )
        ]
    )

    for name in ["testLH1_data_tests", "testLH1_file_tests"]:
        activity = pipeline.get_activity_by_name(name)
        activity.evaluate(state)

        # Note the extra ["value"] before .result
        assert activity.type_properties["parameters"]["ENV"]["value"].result == env