# Create 
configfile: "config.yaml"

import os
from ftplib import FTP
import time

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

def touch_file(fname,update_time_seconds=None):
	"""
	Set the access time and update time of a file. If the file doesn't
	exist, create it. If no update time is set, us the current time.
	"""
	if update_time_seconds is None:
		update_time_seconds=time.time()
	# If the file doesn't exist...
	if not os.path.exists(fname):
		# ... create it.
		ofs=open(fname,"w")
		ofs.close()
	# Set the update time of the file.
	os.utime(fname,(update_time_seconds,update_time_seconds))
	
def get_database_lists():
	dbs=[]
	for fname in ("dna_database_list","protein_database_list"):
		with open(fname) as ifs:
			for item in ifs:
				dbs.append(item.strip())
	return dbs

remote_filenames=[]
database_list=get_database_lists()

def collect_filenames(rec):
	global remote_filenames
	global database_list
	f = rec.strip().split()
	fname=f[-1]
	time_str=' '.join(f[5:8])
	basename=fname.split('.')[0]
	if basename in database_list and fname.endswith(".tar.gz"):
		print(rec)
		print(fname)
		remote_filenames.append(fname)
		touch_file(fname,CalcTime(time_str))

onstart:
	# For each file in the blast/db directory at the FTP site,
	# create a local file with size 0 bytes with the same 
	# create/update time as the local file. These will be
	# local proxy for the remote the source files.

	ftp = FTP(config['ftp_site'])
	ftp.login()
	ftp.cwd(config['ftp_dir'])
	ftp.retrlines('LIST',collect_filenames)

rule all:
	output: touch("done")
