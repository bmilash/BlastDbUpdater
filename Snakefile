# Snakefile - snakemake workflow for downloading and installing blast databases.
# The strategy is to create an empty, local proxy file for each remote database
# file that has the same update time as its remote counterpart. These local files
# will comprise the "source" from which the local database files will be built.
# The local proxy files are created by the onstart: event handler.
# If the local database files are out of date with respect to the local proxy
# files, the remote files will be downloaded and installed.
# Downloading will include MD5 checksum checking.
# Un-tarring and installing is tricky, as there are 4 different types of
# databases: DNA vs Protein, and single-part vs multi-part.
# DNA and Protein database files have different filename extensions.
# Single-part and multi-part database filenames differ in that multi-part
# database filenames include a part number, for example human_genomic.<part>.<extension>.
# The multi-part databases also have a ".nal" or ".pal" component that links all
# the parts together.
#
# Given that there are 4 different types of databases, the top level rule will have
# 4 inputs, one for each database type.
configfile: "config.yaml"

import os
from ftplib import FTP
import time

def ReadDbList(filename):
	"""
	ReadDbList(filename) - reads a database list from the named file,
	and returns the database names as a list of stripped strings.
	"""
	dblist=[]
	with open(filename) as dblist_file:
		for dbname in dblist_file:
			if not dbname.startswith('#'):
				dblist.append(dbname.strip())
	return dblist
	
def get_database_lists():
	return ReadDbList("dna_database_list") + ReadDbList("protein_database_list")

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

remote_filenames=[]
database_list=get_database_lists()
dna_database_list=ReadDbList("dna_database_list")
database_pieces={}

def RecordPiece(basename,piece):
	global database_pieces
	if basename not in database_pieces:
		database_pieces[basename]=[]
	if piece is not None:
		database_pieces[basename].append(piece)

def CreateLocalFile(rec):
	global remote_filenames
	global database_list
	if rec.startswith('#'):
		return
	f = rec.strip().split()
	fname=f[-1]
	time_str=' '.join(f[5:8])
	basename=fname.split('.')[0]
	if basename in database_list and fname.endswith(".tar.gz"):
		remote_filenames.append(fname)
		if len(fname.split('.')) == 4:
			# Database has multiple pieces (00, 01, etc)
			RecordPiece(basename,fname.split('.')[1])
		else:
			# Database has a single piece.
			RecordPiece(basename,None)
		touch_file(fname,CalcTime(time_str))

# File extensions for protein and nucleotide database files. These
# are found on the separate parts (00,01,...) of the databases, and
# dont include the .pal and .nal files that are present just once
# for a given database.
protein_extensions=[ 'phd',
	'phi',
	'phr',
	'pin',
	'pnd',
	'pni',
	'pog',
	'ppd',
	'ppi',
	'psd',
	'psi',
	'psq',
]

dna_extensions=[ 'nhd',
	'nhi',
	'nhr',
	'nin',
	'nnd',
	'nni',
	'nog',
	'nsd',
	'nsi',
	'nsq',
]

wildcard_constraints:
	piece="\d+"

def CreateDirectories():
	for subdir in ['ShadowTargz','Download','LocalTargz','LocalDb']:
		if not os.path.exists(subdir):
			os.mkdir(subdir)

onstart:
	# For each file in the blast/db directory at the FTP site,
	# create a local file with size 0 bytes with the same 
	# create/update time as the local file. These will be
	# local proxy for the remote the source files. Only .tar.gz
	# files are created, and only for the databases of interest.
	CreateDirectories()
	ftp = FTP(config['ftp_site'])
	ftp.login()
	ftp.cwd(config['ftp_dir'])
	ftp.retrlines('LIST',CreateLocalFile)
	print(f"remote_filenames: {remote_filenames}")
	print(f"Database pieces: {database_pieces}")

rule all:
	input: expand("LocalTargz/{targzfile}", targzfile=remote_filenames)
	output: touch("all.done")

rule download_all:
	input: expand("ShadowTargz/{targzfile}", targzfile=remote_filenames)
	output: expand("LocalTargz/{targzfile}", targzfile=remote_filenames)

rule download_one:
	output: expand("ShadowTargz/{targzfile}", targzfile=remote_filenames)
	run:

rule all_dna:
	message: "Rule {rule}: updating DNA databases."
	input: expand("dna_{database}.done", database=dna_database_list)
	output: touch("dna_all.done")

rule one_dna:
	message: "Rule {rule}: updating dna database {database}."
	input: expand("LocalDb/{database}.{suffix}",suffix=dna_extensions)
	output: touch("dna_{database}.done")

rule untar_dna:
	message: "Rule {rule}: un-tarring {input}"
	input: "LocalTargz/{database}.tar.gz"
	output: expand("LocalDb/{database}.{suffix}",suffix=dna_extensions)
	shell: "cd LocalDb; tar xvfz ../{input}"

rule clean:
	shell: "rm -rf dna_all.done"
