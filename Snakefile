# Snakefile - snakemake workflow for downloading and installing blast databases.
# The strategy is to create an empty, local proxy file for each remote database
# file that has the same update time as its remote counterpart. These local files
# will comprise the "source" from which the local database files will be built.
# The local proxy files are created in top-level code in the Snakefile.
# If the local database files are out of date with respect to the local proxy
# files, the remote files will be downloaded and installed.
# Downloading will include MD5 checksum checking.
# Un-tarring and installing is tricky, as there are 3 different types of
# databases: DNA single part, DNA multipart, and protein multipart.
# DNA and Protein database files have different filename extensions.
# Single-part and multi-part database filenames differ in that multi-part
# database filenames include a part number, for example human_genomic.<part>.<extension>.
# The multi-part databases also have a ".nal" or ".pal" component that links all
# the parts together.
# It seems there are no single-part protein databases. The smaller protein databases
# use the multipart format (with part number 00 in the file names). Too bad the
# small DNA databases don't do the same thing!
#
# Given that there are 3 different types of databases, the top level rule will have
# 3 inputs, one for each database type.
configfile: "config.yaml"

import os
from ftplib import FTP
import time
import sys

class DbInventory():
	"""
	DbInventory objects keep track of all the target databases and
	whether they are DNA or protein, single part or multi part, and
	how many parts they have.
	"""
	def __init__(self):
		self.dna_databases=set()
		self.protein_databases=set()
		self.single_part=set()
		self.multi_part=set()
		self.db_part_count=dict()
		self.db_parts=dict()
	
	def __str__(self):
		return f"DbInventory: dna_databases: {self.dna_databases} protein_databases: {self.protein_databases} single_part: {self.single_part} multi_part: {self.multi_part} db_part_count: {self.db_part_count} db_parts: {self.db_parts}"
	
	def ListSinglepartDNA(self):
		dbnames=[]
		for dbname in self.dna_databases:
			if self.is_singlepart(dbname):
				dbnames.append(dbname)
		return dbnames
	
	def ListMultipartProtein(self):
		dbnames=[]
		for dbname in self.protein_databases:
			if self.is_multipart(dbname):
				dbnames.append(dbname)
		return dbnames
	
	def ListMultipartDNA(self):
		dbnames=[]
		for dbname in self.dna_databases:
			if self.is_multipart(dbname):
				dbnames.append(dbname)
		return dbnames

	def ListDbParts(self,dbname):
		"Returns a list of the db part names (00,01,etc) for the given database."
		parts=[f"{i:02d}" for i in range(0,self.db_part_count[dbname])]
		return parts

	def is_dna(self,dbname):
		return dbname in self.dna_databases
	
	def is_protein(self,dbname):
		return dbname in self.protein_databases
	
	def is_singlepart(self,dbname):
		return self.db_part_count[dbname] == 1
	
	def is_multipart(self,dbname):
		return self.db_part_count[dbname] > 1
	
	def add_part(self,dbname,part_name):
		try:
			self.db_part_count[dbname]+=1
			self.single_part.remove(dbname)
			self.multi_part.add(dbname)
			self.db_parts[dbname].append(part_name)
		except KeyError:
			self.db_part_count[dbname]=1
			self.single_part.add(dbname)
			self.db_parts[dbname]=[part_name]
	
	def __contains__(self,item):
		"""
		This method enables 'database in dbinventory'.
		"""
		return item in self.dna_databases or item in self.protein_databases

	def ReadDbList(self,filename,dna=True):
		"""
		Reads a database list from the named file,
		and stores the database names
		"""
		dblist=[]
		with open(filename) as dblist_file:
			for dbname in dblist_file:
				if dbname.startswith('#'):
					continue
				if dna:
					self.dna_databases.add(dbname.strip())
				else:
					self.protein_databases.add(dbname.strip())
	
	def DbList(self,dblist,dna=True):
		"""
		Stores the given list of databases, whether dna or protein.
		"""
		if dna:
			self.dna_databases = dblist[:]
		else:
			self.protein_databases = dblist[:]
	

# Create and initialize the database inventory.
dbinventory=DbInventory()
#dbinventory.ReadDbList("dna_database_list",dna=True)
#dbinventory.ReadDbList("protein_database_list",dna=False)
dbinventory.DbList(config['dna_databases'],dna=True)
dbinventory.DbList(config['protein_databases'],dna=False)

def CalcTime(time_str):
	"""
	Returns time in seconds since the epoch from the string time_str.
	time_str is format Month day year or Month day hh:mm.
	"""
	f = time_str.strip().split()
	months={
		'Jan':1,
		'Feb':2,
		'Mar':3,
		'Apr':4,
		'May':5,
		'Jun':6,
		'Jul':7,
		'Aug':8,
		'Sep':9,
		'Oct':10,
		'Nov':11,
		'Dec':12,
	}
	month=months[f[0]]
	day=int(f[1])
	second=0
	is_dst=-1
	if ':' in f[2]:
		hour=int(f[2][0:2])
		minute=int(f[2][3:])
		year=time.localtime(time.time()).tm_year
		if month > time.localtime(time.time()).tm_mon:
			# Must be previous year.
			year-=1
	else:
		hour=0
		minute=0
		year=int(f[2])
	return time.mktime((year,month,day,hour,minute,second,0,0,is_dst))

def touch_file(fname,update_time_seconds=None,directory="ShadowTargz"):
	"""
	Set the access time and update time of a file. If the file doesn't
	exist, create it. If no update time is set, us the current time.
	"""
	fname=os.path.join(directory,fname)
	if update_time_seconds is None:
		update_time_seconds=time.time()
	# If the file doesn't exist...
	if not os.path.exists(fname):
		# ... create it.
		ofs=open(fname,"w")
		ofs.close()
	# Set the update time of the file.
	os.utime(fname,(update_time_seconds,update_time_seconds))


def CreateLocalFile(rec):
	global dbinventory
	if rec.startswith('#'):
		return
	f = rec.strip().split()
	fname=f[-1]
	time_str=' '.join(f[5:8])
	basename=fname.split('.')[0]
	if basename in dbinventory and fname.endswith(".tar.gz"):
		touch_file(fname,CalcTime(time_str))
		# Parse file name to get part string, and
		# add that to dbinventory.db_parts dict.
		z=fname.split('.')
		if len(z)==4:
			# Has a part string.
			part_name=z[1]
		else:
			part_name=''
		dbinventory.add_part(basename,part_name)

# File extensions for protein and nucleotide database files. These
# are found on the separate parts (00,01,...) of the databases, and
# dont include the .pal and .nal files that are present just once
# for a given database. These aren't complete lists of the extensions,
# just the extensions that all databases have. Doing this because there's
# no rhyme or reason to which databases have or don't have which extensions.
protein_extensions=[ 'phr', 'pin', 'pnd', 'pni', 'pog', 'ppd', 'ppi', 'psd', 'psi', 'psq' ]

dna_extensions=[ 'nhr', 'nin', 'nnd', 'nni', 'nsd', 'nsi', 'nsq' ]

wildcard_constraints:
	part="\d+",
	dbname="[0-9A-Za-z_]*"

def CreateDirectories():
	for subdir in ['ShadowTargz','Download','DbFiles','Quarantine']:
		if not os.path.exists(subdir):
			os.mkdir(subdir)

# For each file in the blast/db directory at the FTP site,
# create a local file with size 0 bytes with the same 
# create/update time as the local file. These will be
# local proxy for the remote the source files. Only .tar.gz
# files are created, and only for the databases of interest.
# Side effect of this is to determine which databases are single-
# part and which are multi-part.
CreateDirectories()
ftp = FTP(config['ftp_site'])
ftp.login()
ftp.cwd(config['ftp_dir'])
ftp.retrlines('LIST',CreateLocalFile)
print(dbinventory,file=sys.stderr)

rule all:
	message: "Rule {rule}: all databases updated."
	input: "dna_sp.done", "dna_mp.done", "protein_multipart.done"

# --- Protein multipart rules ---

rule all_protein_multipart:
	message: "Rule {rule}: all multipart protein databases updated."
	input: expand("{dbname}_mp_protein.done", dbname=dbinventory.ListMultipartProtein())
	output: touch("protein_multipart.done")

rule one_protein_multipart_all:
	message: "Rule {rule}: updating multipart protein database {wildcards.dbname}."
	input: lambda wildcards: expand("DbFiles/{dbname}.{part}.{suffix}",dbname=["{dbname}"],part=dbinventory.db_parts[wildcards.dbname],suffix=protein_extensions)
	output: touch("{dbname}_mp_protein.done")

rule untar_protein_multipart_single:
	message: "Rule {rule}: untarring {input}."
	input: "Download/{dbname}.{part}.tar.gz"
	output: expand("DbFiles/{dbname}.{part}.{suffix}",dbname=["{dbname}"],part=["{part}"],suffix=protein_extensions)
	shell: "cd DbFiles; tar xvfz ../{input}"

# --- DNA multipart rules ---

rule all_dna_multipart:
	message: "Rule {rule}: all multipart DNA databases updated."
	input: expand("{dbname}_mp.done", dbname=dbinventory.ListMultipartDNA())
	output: touch("dna_mp.done")

rule one_dna_multipart_all:
	message: "Rule {rule}: updating multipart DNA database {wildcards.dbname}."
	input: lambda wildcards: expand("DbFiles/{dbname}.{part}.{suffix}",dbname=["{dbname}"],part=dbinventory.db_parts[wildcards.dbname],suffix=dna_extensions)
	output: touch("{dbname}_mp.done")

rule untar_dna_multipart_single:
	message: "Rule {rule}: untarring {input}."
	input: "Download/{dbname}.{part}.tar.gz"
	output: expand("DbFiles/{dbname}.{part}.{suffix}",dbname=["{dbname}"],part=["{part}"],suffix=dna_extensions)
	shell: "cd DbFiles; tar xvfz ../{input}"

rule download_multipart_single:
	message: "Rule {rule}: downloading {input}."
	params: ftp_site=config['ftp_site'], ftp_dir=config['ftp_dir']
	input: "ShadowTargz/{dbname}.{part}.tar.gz"
	output: "Download/{dbname}.{part}.tar.gz"
	shell: "cd Download; wget {params.ftp_site}/{params.ftp_dir}/{wildcards.dbname}.{wildcards.part}.tar.gz"

# --- DNA singlepart rules ---

rule all_dna_singlepart:
	message: "Rule {rule}: all singlepart DNA databases updated."
	input: expand("{dbname}_sp.done", dbname=dbinventory.ListSinglepartDNA())
	output: touch("dna_sp.done")

rule one_dna_singlepart:
	message: "Rule {rule}: updating singlepart DNA database {wildcards.dbname}."
	input: expand("DbFiles/{dbname}.{suffix}",dbname=["{dbname}"],suffix=dna_extensions)
	output: touch("{dbname}_sp.done")

rule untar_dna_singlepart:
	input: "Download/{dbname}.tar.gz"
	output: expand("DbFiles/{dbname}.{suffix}",dbname=["{dbname}"],suffix=dna_extensions)
	shell: "cd DbFiles; tar xvfz ../{input}"

rule download_singlepart:
	params: ftp_site=config['ftp_site'], ftp_dir=config['ftp_dir']
	input: "ShadowTargz/{dbname}.tar.gz"
	output: "Download/{dbname}.tar.gz"
	shell: """
		cd Download
		wget {params.ftp_site}/{params.ftp_dir}/{wildcards.dbname}.tar.gz
		wget {params.ftp_site}/{params.ftp_dir}/{wildcards.dbname}.tar.gz.md5
		# Test md5 checksum
		remote=`cat {wildcards.dbname}.tar.gz.md5`
		local=`md5sum {wildcards.dbname}.tar.gz`
		if [ "$local" = "$remote" ]
		then
			echo "{wildcards.dbname}.tar.gz.md5 checksum OK."
			exit 0
		else
			echo "{wildcards.dbname}.tar.gz.md5 checksum ERROR."
			mv {wildcards.dbname}.tar.gz ../Quarantine
			exit 1
		fi
		"""

# --- other rules ---
rule clean:
	shell: "rm -rf *.done DbFiles Download ShadowTargz Quarantine"
