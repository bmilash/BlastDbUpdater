Bootstrap:docker
From:snakemake/snakemake:v5.7.0

%labels
Maintainer brett.milash@utah.edu
Version 1.0.0

%files
# Copy these files to / in the container.
./config.yaml
./Snakefile

%environment

%runscript
cd /data 
echo "Files in /data:"
ls /data
echo "Generating directed acyclic graph diagram -> dag.png"
snakemake --dag | dot -Tpng > dag.png
echo "Generating rule graph diagram -> rulegraph.png"
snakemake --rulegraph | dot -Tpng > rulegraph.png
echo "Running workflow in dry-run mode."
snakemake -n

%post
mkdir /data
mv /Snakefile /data/Snakefile
mv /config.yaml /data/config.yaml
