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
exec snakemake

%post
