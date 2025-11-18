from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--workspace-id", required=True)
parser.add_argument("--environment", required=True)
parser.add_argument("--repo-dir", required=True)
parser.add_argument("--items-in-scope", required=True)       # "Lakehouse,Notebook,DataPipeline,SemanticModel,Report"
args = parser.parse_args()


item_type_in_scope = [x.strip() for x in args.items_in_scope.split(",")]

target = FabricWorkspace(
    workspace_id=args.workspace_id,
    environment=args.environment,
    repository_directory=args.repo_dir,
    item_type_in_scope=item_type_in_scope
)

publish_all_items(target)

unpublish_all_orphan_items(target)