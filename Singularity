Bootstrap:docker
From:snakemake/snakemake:v5.7.0

%labels
Maintainer brett.milash@utah.edu
Version 1.0.0

%files
./config.yaml ./config.yaml
./Snakefile ./Snakefile

%environment

%runscript
echo "Generating directed acyclic graph diagram -> dag.png"
snakemake --dag | dot -Tpng > dag.png
echo "Generating rule graph diagram -> rulegraph.png"
snakemake --rulegraph | dot -Tpng > rulegraph.png
echo "Running workflow in dry-run mode."
snakemake -n

%post
