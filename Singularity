Bootstrap:docker
From:snakemake/snakemake:v5.7.0

%labels
Maintainer brett.milash@utah.edu
Version 1.0.0

%files
# Copy these files to / in the container.
./config.yaml
./Snakefile

%runscript
# Determine configuration file.
if [ $# = 0 ]
then
	# No arguments. Use the default configuration file in /data.
	configfile=/data/config.yaml
else
	# Use first argument as config file. Exit if it doesn't exist.
	configfile=$1
	echo "Using configuration file $configfile."
	if [ ! -f $configfile ]
	then
		echo "Configuration file $configfile doesn't exist. Exiting."
		exit 1
	fi
fi
echo "Generating directed acyclic graph diagram -> dag.png"
snakemake -s /data/Snakefile --configfile $configfile --dag | dot -Tpng > dag.png
echo "Generating rule graph diagram -> rulegraph.png"
snakemake --rulegraph | dot -Tpng > rulegraph.png
echo "Running workflow in dry-run mode."
snakemake -n

%post
mkdir /data
mv /Snakefile /data/Snakefile
mv /config.yaml /data/config.yaml
