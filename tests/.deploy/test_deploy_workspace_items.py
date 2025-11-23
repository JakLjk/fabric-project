import os
from pathlib import Path
import runpy

ROOT = Path(__file__).resolve().parents[2]

def test_deploy_workspace_items_builds_correct_path(monkeypatch, tmp_path):
    script = ROOT / ".deploy" / "deploy_workspace_items.py"

    # Fake env vars
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")

    # Monkeypatch deploy_with_config to capture arguments
    calls = {}

    def fake_deploy_with_config(config_file_path, environment, token_credential):
        calls["config_file_path"] = config_file_path
        calls["environment"] = environment

    def fake_append_feature_flag(name):
        pass

    monkeypatch.setitem(runpy._run_code.__globals__ if hasattr(runpy, "_run_code") else {}, "deploy_with_config", fake_deploy_with_config)
    # simpler: patch via import if you restructure script; or wrap code in a function

    # Run script as a module with args
    import sys
    old_argv = sys.argv
    sys.argv = [
        str(script),
        "--environment", "test",
        "--workspace", "engineering",
    ]
    try:
        runpy.run_path(str(script), run_name="__main__")
    finally:
        sys.argv = old_argv

    assert calls["environment"] == "TEST"
    assert calls["config_file_path"].endswith("workspace/engineering/config.yml")
