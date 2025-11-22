4. Testing terminology in CI/CD

Here is what each test type means and why it is placed on a specific branch.

Linting

Purpose: Ensure consistent code style and catch obvious errors early.

Examples:

Missing semicolons.

Poor formatting.

Unused variables.

Dead code.

Runs on:
Every PR into dev, sometimes on feature branches before PR.

Why:
Cheap and fast. Prevents simple mistakes from merging.

Unit tests

Purpose:
Test small pieces of code (functions, classes) in isolation.

Runs on:
Every PR into dev.

Why:
Guarantees basic correctness before integration.

Integration tests

Purpose:
Test how components work together.

Examples in a Fabric context:

Data pipeline triggers a dataflow.

Dataflow writes to lakehouse.

Semantic model refresh picks up the result.

Runs on:
PR from dev to test
or
after merge into test.

Why:
Integration tests assume a more complete environment.

Deployment dry-run

Purpose:

Validate deployment content without actually changing the workspace.

Useful to confirm Fabric CI/CD rules, connections, and structure.

Runs on:
Before deploying to test.

Why:

Detect invalid workspace IDs.

Detect connection mismatches.

Validate parameter files.

Ensure CI/CD pipeline will not break the target environment.

Fabric CI/CD library often supports "validate only" operations.

Smoke tests

Purpose:

Quick checks after deployment to ensure the system is basically working.

Examples in your context:

The pipeline exists in the workspace.

It can be triggered successfully.

The dataflow ApplyChanges job runs without errors.

Runs on:
After merging into main and deploying to production.

Why:

Ensures production is healthy immediately after deployment.