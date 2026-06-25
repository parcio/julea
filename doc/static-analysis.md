# Static Analysis

The codebase is checked for defects using CodeChecker, which wraps several static analysis tools (clang-sa, gcc, infer, cppcheck).

## Pipeline
It is automatically run in the GitHub pipeline and emits an artifact called "CodeChecker Bug Reports".
A small summary of the number and types of errors found is given at the end of the "Generate HTML report" step.
To view the detailed results, the artifact must be downloaded, extracted, and the `index.html` or `statistics.html` file opened.

## Local
Alternatively, the tool can be run locally.
A full guide can be found [here](https://github.com/Ericsson/codechecker/blob/master/docs/usage.md).

Notably, the tool can calculate a diff between two analysis runs as explained [here](https://github.com/Ericsson/codechecker/blob/master/docs/usage.md#using-diff-command-on-the-local-filesystem).
This can be used to check whether your current local changes would introduce or remove any bugs.

In summary:
1. Ensure CodeChecker, the desired static analysis tools, and all JULEA dependencies are installed and available.

2. Generate a `compile_commands.json` file.
```bash
meson setup -Dis_analysis_build=true bld
```

3. Run the analysis.
```bash
CodeChecker analyze ./bld/compile_commands.json -o results
```

4. Parse and view the results.
```bash
CodeChecker parse --export html --output ./reports_html ./results  &&
firefox ./reports_html/index.html
```

## Potential improvements
The current setup is quite bare-bones.
CodeChecker supports running a server to store results of previous analysis runs, tracking reports that have already been marked as known false positives, and automatically calculating a diff between the main branch and the branch to be merged.
