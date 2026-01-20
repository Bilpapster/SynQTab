# JAR Files Directory

**TL; DR**: The JAR files are automatically downloaded when running the `<root_dir>/scripts/prepare-execution-environment.sh` script.
No further action is required on your side.

In case you wish to separately download the JARs, feel free to use the provided script:
```bash
# from the root directory of the project
bash scripts/download_jars.sh
# the files can then be found in `<root_dir>/synqtab/jars/`, i.e., the directory that this README lives.
```

## JAR Details

- `HyFD.jar` - The HyFD functional dependency discovery algorithm
  - Sourced from https://hpi.de/naumann/projects/data-profiling-and-analytics/metanome-data-profiling/algorithms.html 
  
- `metanome-cli.jar` - Metanome command-line interface required to run HyFD
  - Sourced from https://github.com/sekruse/metanome-cli/releases/tag/v1.1.0

Our project depends on the aforementioned JAR files to perform functional dependency discovery (FDD). We use FDD as a
metric to evaluate the quality of the synthetically generated tabular data. More information can be found in our paper.
