#!/usr/bin/env python3

import sys
import psycopg2
import os
import shutil
import subprocess
import configparser
import argparse
import time
import re
import pkg_resources


#
# Export Chado organism sequence data to GFF file.
#
# See http://mediawiki.internal.sanger.ac.uk/index.php?title=Pathogens_GFF3_Export
#
class ChadoGffExporter:

	def __init__(self, prog_args):
		self.prog_args = prog_args
		self.run_jobs_flag = True

		self.jobtitle="chadoexp"

		self.apolloexport = False

		self.apolloconverterapp = ''
		self.copytoftpsiteflag = True
		self.ftpsitefolder = ''
		self.reportemailaddress = ''
		self.apolloconverterappargs = ''
		self.apollogffpath = ''
		self.checkerjobstartdelay = 10

		self.__gt_filepath_wildcard_escaping = False

	# ------

	@property
	def jobtitle_property(self):
		return self.jobtitle

	@jobtitle_property.setter
	def jobtitle_property(self, value):
		self.jobtitle = value

	# ------

	@property
	def configfile_property(self):
		return self.configfile

	@configfile_property.setter
	def configfile_property(self, value):
		self.configfile = value

	# ------

	@property
	def gtbin_property(self):
		return self.gtbin

	@gtbin_property.setter
	def gtbin_property(self, value):
		self.gtbin = value

	# ------

	@property
	def writedbentrypath_property(self):
		return self.writedbentrypath

	@writedbentrypath_property.setter
	def writedbentrypath_property(self, value):
		self.writedbentrypath = value

	# ------

	@property
	def org_list_file_property(self):
		return self.org_list_file

	@org_list_file_property.setter
	def org_list_file_property(self, value):
		self.org_list_file = value

	# ------

	@property
	def slice_size_property(self):
		return self.slice_size

	@slice_size_property.setter
	def slice_size_property(self, value):
		self.slice_size = value

	# ------

	@property
	def queue_property(self):
		return self.queue

	@queue_property.setter
	def queue_property(self, value):
		self.queue = value

	# ------

	@property
	def targetpath_property(self):
		return self.targetpath

	@targetpath_property.setter
	def targetpath_property(self, value):
		self.targetpath = value

	# ------

	@property
	def finalresultpath_property(self):
		return self.finalresultpath

	@finalresultpath_property.setter
	def finalresultpath_property(self, value):
		self.finalresultpath = value

	# ------

	@property
	def scriptpath_property(self):
		return self.scriptpath

	@scriptpath_property.setter
	def scriptpath_property(self, value):
		self.scriptpath = value

	# ------

	@property
	def logpath_property(self):
		return self.logpath

	@logpath_property.setter
	def logpath_property(self, value):
		self.logpath = value

	# ------

	@property
	def statuspath_property(self):
		return self.statuspath

	@statuspath_property.setter
	def statuspath_property(self, value):
		self.statuspath = value

	# ------

	@property
	def resultbasepath_property(self):
		return self.resultbasepath

	@resultbasepath_property.setter
	def resultbasepath_property(self, value):
		self.resultbasepath = value

	# ------

	@property
	def apolloconverterapp_property(self):
		return self.apolloconverterapp

	@apolloconverterapp_property.setter
	def apolloconverterapp_property(self, value):
		self.apolloconverterapp = value

	# ------

	@property
	def apolloconverterappargs_property(self):
		return self.apolloconverterappargs

	@apolloconverterappargs_property.setter
	def apolloconverterappargs_property(self, value):
		self.apolloconverterappargs = value

	# ------

	@property
	def copytoftpsiteflag_property(self):
		return self.copytoftpsiteflag

	@copytoftpsiteflag_property.setter
	def copytoftpsiteflag_property(self, value):
		self.copytoftpsiteflag = value

	# ------

	@property
	def ftpsitefolder_property(self):
		return self.ftpsitefolder

	@ftpsitefolder_property.setter
	def ftpsitefolder_property(self, value):
		self.ftpsitefolder = value

	# ------

	@property
	def reportemailaddress_property(self):
		return self.reportemailaddress

	@reportemailaddress_property.setter
	def reportemailaddress_property(self, value):
		self.reportemailaddress = value

	# ------

	@property
	def apollogffpath_property(self):
		return self.apollogffpath

	@apollogffpath_property.setter
	def apollogffpath_property(self, value):
		self.apollogffpath = value

	# ------

	@property
	def apolloexport_property(self):
		return self.apolloexport

	@apolloexport_property.setter
	def apolloexport_property(self, value):
		self.apolloexport = value

	# ------

	@property
	def dump_all_property(self):
		return self.dump_all

	@dump_all_property.setter
	def dump_all_property(self, value):
		self.dump_all = value

	# ------

	@property
	def run_jobs(self):
		return self.run_jobs_flag

	@run_jobs.setter
	def run_jobs(self, value):
		self.run_jobs_flag = value

	# ------

	@property
	def checkerjobstartdelay_property(self):
		return self.checkerjobstartdelay

	@checkerjobstartdelay_property.setter
	def checkerjobstartdelay_property(self, value):
		self.checkerjobstartdelay = value

	# ------

	@property
	def gt_filepath_wildcard_escaping_property(self):
		return self.__gt_filepath_wildcard_escaping

	@gt_filepath_wildcard_escaping_property.setter
	def gt_filepath_wildcard_escaping_property(self, value):
		self.__gt_filepath_wildcard_escaping = value

	#
	# Top-level function to run the export
	#
	def run(self):

		self.read_program_arguments(self.prog_args)
		self.validate_arguments()
		self.read_configuration()
		self.validate_config()
		#self.display_configuration()
		self.create_folder_structure()
		self.execute_export()


	#
	# Get command line arguments.
	#
	def read_program_arguments(self, prog_args):

		parser = argparse.ArgumentParser(prog=prog_args[0], description='Script to export Chado database organism data to GFF files.')
		parser.add_argument('-i', help='Path of script configuration file', required=True, dest='configfile')
		parser.add_argument('-a', help='Export all public Chado organisms to GFF (overrides -f option)', required=False, action='store_true', dest='dump_all')
		parser.add_argument('-f', help='A file containing a custom list of organisms to export from Chado', required=False, dest='org_list_file', default='generate_gff_from_chado.orglist')

		args = parser.parse_args(prog_args[1:])
		self.configfile=args.configfile.strip()
		self.dump_all=args.dump_all
		self.org_list_file=args.org_list_file.strip()


	#
	# Validate the program arguments
	#
	def validate_arguments(self):

		if len(self.configfile) == 0 or not os.path.isfile(self.configfile):
			print('Configuration file not found: %s' % (self.configfile))
			exit(1)

		if self.dump_all == False:
			if len(self.org_list_file) == 0 or not os.path.isfile(self.org_list_file):
				print('Organism file not found: %s' % (self.org_list_file))
				exit(1)

	#
	# Read configuration from config file.
	#
	def read_configuration(self):

		config = configparser.ConfigParser()
		self.config = config

		try:
			config.read(self.configfile)

			self.gtbin = config.get('General', 'genome_tools_bin')
			self.writedbentrypath = config.get('General', 'write_db_entry_path')

			# number of genomes per chunk
			self.slice_size = int(config.get('Job', 'slice_size'))

			# target queue
			self.queue = config.get('Job', 'queue')

			# working directory
			self.targetpath = config.get('General', 'target_path').strip()
			self.finalresultpath = self.targetpath + "/results"
			self.scriptpath = self.targetpath + "/scripts"
			self.logpath = self.targetpath + "/logs"
			self.statuspath = self.targetpath + "/status"
			self.resultbasepath = self.targetpath + "/artemis/GFF"

		except (configparser.NoSectionError, configparser.MissingSectionHeaderError) as e:
			print('Properties file is missing mandatory sections: %s' % str(e))
			exit(1)

		try:
			self.jobtitle = config.get('Job', 'name')
		except Exception as err:
			# Fall back to default name
			pass

		try:
			self.__gt_filepath_wildcard_escaping = (config.get('General', 'gt_filepath_wildcard_escaping') == "True")
		except Exception as err:
			# Fall back to default value
			pass

		# Read any properties related to Apollo export
		self.read_apollo_export_configuration(config)


	#
	# Validate the program ini file configuration
	#
	def validate_config(self):

		valid = True

		if len(self.targetpath) <= 1 or (not os.path.isdir(self.targetpath)):
			print('Configuration file target_path property does not point to a valid directory: %s' % (self.targetpath))
			valid = False

		if len(self.gtbin) == 0 or shutil.which(self.gtbin) is None:
			print('Configuration file genome_tools_bin property is not valid: %s' % (self.gtbin))
			valid = False

		if len(self.writedbentrypath) == 0 or shutil.which(self.writedbentrypath) is None:
			print('Configuration file write_db_entry_path property is not valid: %s' % (self.writedbentrypath))
			valid = False

		if self.apolloexport == True:
			if len(self.apolloconverterapp) == 0 or shutil.which(self.apolloconverterapp) is None:
				print('Configuration file apollo_gff_converter_app_path property is not valid: %s' % (self.apolloconverterapp))
				valid = False

			if self.copytoftpsiteflag == True and (len(self.ftpsitefolder) == 0 or not os.path.isdir(self.ftpsitefolder)):
				print('Configuration file ftp_site_folder property does not point to a valid directory: %s' % (self.ftpsitefolder))
				valid = False

			if not re.match("[^@]+@[^@]+\.[^@]+", self.reportemailaddress):
				print('Configuration file report_email_address property is not a valid email address: %s' % (self.reportemailaddress))
				valid = False

			if self.checkerjobstartdelay < 1:
				print('Configuration file checker_job_start_delay_secs property is not permitted to be less than 1 second: %s' % (self.checkerjobstartdelay))
				valid = False

		if valid == False:
			exit(1)

	#
	# Print out the configuration
	#
	def display_configuration(self):

		print("dump_all property: %s" % self.dump_all)
		print("gtbin property: %s" % self.gtbin)
		print("writedbentrypath property: %s" % self.writedbentrypath)
		print("org_list_file property: %s" % self.org_list_file)
		print("slice_size property: %d" % self.slice_size)
		print("queue property: %s" % self.queue)
		print("targetpath property: %s" % self.targetpath)
		print("finalresultpath property: %s" % self.finalresultpath)
		print("scriptpath property: %s" % self.scriptpath)
		print("logpath property: %s" % self.logpath)
		print("statuspath property: %s" % self.statuspath)
		print("resultbasepath property: %s" % self.resultbasepath)
		print("dbname property: %s" % self.config.get('Connection', 'database'))
		print("user property: %s" % self.config.get('Connection', 'user'))
		print("host property: %s" % self.config.get('Connection', 'host'))
		print("password property: %s" % self.config.get('Connection', 'password'))
		print("port property: %s" % self.config.get('Connection', 'port'))
		print("Apollo export flag: %s" % self.apolloexport)
		print("apolloconverterapp property: %s" % self.apolloconverterapp)
		print("apolloconverterappargs property: %s" % self.apolloconverterappargs)
		print("apollogffpath property: %s" % self.apollogffpath)
		print("copytoftpsiteflag property: %s" % self.copytoftpsiteflag)
		print("ftpsitefolder property: %s" % self.ftpsitefolder)
		print("checkerjobstartdelay property: %s" % str(self.checkerjobstartdelay))
		print("gt_filepath_wildcard_escaping_property: %s" % self.__gt_filepath_wildcard_escaping)

	#
	# Read a bespoke list of organisms to export, from file.
	#
	def read_organism_list_from_file(self):

		results = []

		with open(self.org_list_file, "r") as f:

			for line in f:
				next = line.strip()
				if next.startswith('#') or next == '':
					continue
				else:
					results.append(next)

		return results


	#
	# Run a bash shell process.
	#
	def run_bash(self, cmd):
		subprocess.Popen(cmd, shell=True, executable='/bin/bash')


	#
	# Connect to the database.
	#
	def open_database_connection(self):

		try:

			self.conn = psycopg2.connect("dbname='" + self.config.get('Connection', 'database') + "' " +
	                            "user='" + self.config.get('Connection', 'user') + "' " +
	                            "host='" + self.config.get('Connection', 'host') + "' " +
	                            "password='" + self.config.get('Connection', 'password') + "' " +
	                            "port='" + self.config.get('Connection', 'port') + "'")
			self.conn.autocommit = True

		except Exception as err:
			print("Unable to connect to the database: %s" % str(err))
			exit(1)


	#
	# Close the database connection.
	#
	def close_database_connection(self):

		try:

			self.conn.close()

		except Exception as err:
			print("Unable to close database connection: %s" % str(err))


	#
	# Create any required directories and clean up any old ones.
	#
	def create_folder_structure(self):

		if self.targetpath == '' or self.targetpath == '/':
			raise Exception('The target_path GFF file directory has not been set in the configuration file. Please create it or change it in the configuration file, and then re-run.')

		if not os.path.isdir(self.targetpath):
			raise Exception('The target GFF file directory ' + self.targetpath + ' does not exist. Please create it or change it in the configuration file, and then re-run.')

		# make dirs if required
		for dir in [self.statuspath, self.logpath, self.scriptpath, self.finalresultpath]:
			if not os.path.isdir(dir):
				os.makedirs(dir)

		# clean up old status files
		for root, dirs, files in os.walk(self.statuspath, topdown=False):
			for name in files:
				os.unlink(os.path.join(root, name))
		# clean up old script files
		for root, dirs, files in os.walk(self.scriptpath, topdown=False):
			for name in files:
				os.unlink(os.path.join(root, name))
		# clean up old log files
		for root, dirs, files in os.walk(self.logpath, topdown=False):
			for name in files:
				os.unlink(os.path.join(root, name))

		# Make any Apollo export related directories if required
		# and ensure we clear out any old files.
		if self.apolloexport == True:

			self.create_apollo_export_folders()

			# Clear out any old files
			for root, dirs, files in os.walk(self.apollogffpath, topdown=False):
				for name in files:
					os.unlink(os.path.join(root, name))

	#
	# Make any Apollo export related directories
	#
	def create_apollo_export_folders(self):

		# Result files folder
		if not os.path.isdir(self.apollogffpath):
			os.makedirs(self.apollogffpath)

		# Target ftp site folder
		if self.copytoftpsiteflag == True and not os.path.isdir(self.ftptargetfolder):
			os.makedirs(self.ftptargetfolder)

	#
	# Escape wildcards in the given gt path if necessary.
	#
	#
	def escape_gt_wildcards(self, path):

		if self.__gt_filepath_wildcard_escaping:
			return path.replace("*", "\\*")

		return path

	#
	# Delivers list of annotated organisms to export,
	# in manageable chunks using yield.
	#
	def get_organism_list(self, size):

		rows = []

		if self.dump_all == True:

			self.open_database_connection()

			try:

				cur = self.conn.cursor()
				cur.execute("select o.common_name as commonName " +
			                "from organismprop op " +
			                "left join cvterm cv on op.type_id = cv.cvterm_id " +
			                "left join organism o on o.organism_id = op.organism_id " +
			                "where cv.name = 'genedb_public' and op.value = 'yes';")
				rows = cur.fetchall()

			finally:
				cur.close()
				self.close_database_connection()

			for sl in zip(*(iter(rows),) * size):
				res = []
				for elem in sl:
					if elem is None:
						continue
					res.append(str(elem[0]))
				yield(res)

		else:
			rows = self.read_organism_list_from_file()
			for i in range(0, len(rows), size):
				yield rows[i:i + size]


	#
	# Export the specified organism sequences to GFF from Chado.
	# Creates export bash scripts and runs them on LSF.
	#
	def execute_export(self):

		jobs = []
		donefiles = []
		errorlogs = []
		orgs = []

		i = 0

		# generate batch jobs and submit them
		for sl in self.get_organism_list(self.slice_size):

			i = i + 1
			runstr = "writedb_entries.py -v -w "+ self.writedbentrypath + " "
			scriptname = "%d__" % i
			for org in sl:
				orgs.append(org)
				runstr = runstr + " -o " + org + " "
				scriptname = scriptname + org

			runstr = runstr + " -x " + self.targetpath + " -d " + self.configfile + " -f 3000"

			tf = open(self.scriptpath + "/" + scriptname, "w+")
			# construct per-node script
			tf.write("#!/bin/bash\n")

			tf.write("JOB_ERROR_STATUS=0\n")

			for org in sl:
				orgpath = self.resultbasepath + "/" + org
				tf.write("rm -rf \"" + orgpath + "\"\n")

			tf.write(runstr + "\n")

			for org in sl:
				orgpath = self.resultbasepath + "/" + org
				# flatten directory structure
				tf.write("find " + orgpath + " -type f " +
					"-exec mv {} " + orgpath + " \\;\n")
				# clean up logs
				tf.write("rm -f " + orgpath + "/tidylog.log\n")
				# remove empty dirs
				tf.write("rmdir --ignore-fail-on-non-empty " + orgpath + "/?\n")

				# merge GFFs into one file per organism
				search_path = self.escape_gt_wildcards("/*.gff.gz")
				tf.write("GT_RETAINIDS=yes " + self.gtbin + " gff3 -sort -tidy -force -retainids -o " +
					orgpath + "/" + org + ".gff3.gz -gzip " + orgpath + search_path + " 2> " + orgpath + "/" + org + ".tidylog \n")

				# allow access to pathdev members
				tf.write("chmod -R 775 " + orgpath + "\n")
				# move result to separate directory
				tf.write("cp " + orgpath + "/" + org + ".gff3.gz " + self.finalresultpath + "\n")
				tf.write("cp " + orgpath + "/" + org + ".tidylog " + self.finalresultpath + "\n")

				if self.apolloexport == False:
					# split sequences and annotations
					tf.write("GT_RETAINIDS=yes " + self.gtbin + " inlineseq_split -seqfile "+ self.finalresultpath + "/" + org + ".genome.fasta -gff3file " + self.finalresultpath + "/" + org + ".noseq.gff3 " + self.finalresultpath + "/" + org + ".gff3.gz\n")
					# gzip everything
					tf.write("gzip -f " + self.finalresultpath + "/" + org + ".genome.fasta \n")
					tf.write("gzip -f " + self.finalresultpath + "/" + org + ".noseq.gff3 \n")
					# prepare cDNA and protein sequences
					tf.write("GT_RETAINIDS=yes " + self.gtbin + " extractfeat -type CDS -join -translate -retainids -seqfile "+ self.finalresultpath + "/" + org + ".genome.fasta.gz -matchdescstart -force -o " + self.finalresultpath + "/" + org + ".prot.fasta.gz -gzip " + self.finalresultpath + "/" + org + ".noseq.gff3.gz \n")
					tf.write("GT_RETAINIDS=yes " + self.gtbin + " extractfeat -type mRNA -retainids -seqfile "+ self.finalresultpath + "/" + org + ".genome.fasta.gz -matchdescstart -force -o " + self.finalresultpath + "/" + org + ".cdna.fasta.gz -gzip " + self.finalresultpath + "/" + org + ".noseq.gff3.gz \n")
					# clean up indices
					tf.write("rm -f " + self.finalresultpath + "/" + org + ".genome.fasta.gz.* \n")
					tf.write("chmod -R 777 " + self.finalresultpath + " \n")

				# Clean-up working writedbentry files to save disk space
				tf.write("rm -rf \"" + orgpath + "\"\n")

				#
				# If this export is for Apollo then we must convert the gff file features to the correct parent-child relationship
				# and copy to ftp site.
				#
				if self.apolloexport == True:

					inputfile = self.finalresultpath + "/" + org + ".gff3.gz"
					outputfile = self.apollogffpath + "/" + org + ".gff3.gz"

					tf.write(self.construct_apollo_converter_app_cmds(inputfile, outputfile) + "\n")

					if self.copytoftpsiteflag == True:
						tf.write("if [[ -s \"" + outputfile + "\" ]]; then\n")
						tf.write("   cp " + outputfile + " " + self.ftptargetfolder + "/" + "\n")
						tf.write("fi\n")

				tf.write("\n")

			donefile = self.statuspath + "/" + scriptname + ".done"

			tf.write("touch " + donefile + "\n")
			tf.close()

			os.chmod(self.scriptpath + "/" + scriptname, 0o775)

			errorlog = self.logpath + "/" + scriptname + ".e"
			jobid = self.jobtitle + str(i)

			# Create LSF job execution string
			execline = "source /etc/bashrc; bsub -J " + jobid + " -q " + self.queue + " -n4  " + \
		              "-R 'select[mem>3500] rusage[mem=3500] span[hosts=1]' -M 3500 " + \
		              "-o " + self.logpath + "/" + scriptname  + ".o " + \
		              "-e " + errorlog + " " + \
		              str(self.scriptpath) + "/" + str(scriptname)

			# Keep track of the jobs that we need to monitor...
			donefiles.append(donefile)
			errorlogs.append(errorlog)
			jobs.append(jobid)

			# Submit script to LSF
			if self.run_jobs_flag == True:
				print("starting job %d -- %s" % (i, scriptname))
				self.run_bash(execline)

		# Submit dependent "completion checker" job.
		# This job runs when all export jobs have finished.
		# We add a small delay as it's possible for the checker to
		# sometimes get scheduled before export jobs and consequently
		# exit immediately (race condition).
		#
		if len(jobs) > 0 and self.apolloexport == True:
			print("waiting to start chado export completion checker job...")
			time.sleep(self.checkerjobstartdelay)
			self.run_checker_job(jobs, donefiles, errorlogs);


	#
	# Create a job that runs upon completion of the export jobs.
	# It checks successful completion and emails a report.
	#
	def run_checker_job(self, jobs, donefiles, errorlogs):

		if len(donefiles) == 0:
			return

		checkerjobname = "chk-" + self.jobtitle
		checkerjobscript = self.scriptpath + "/" + checkerjobname + ".sh"

		cf = open(checkerjobscript, "w+")
		self.write_checker_job_script(cf, donefiles, errorlogs)
		cf.close()
		os.chmod(checkerjobscript, 0o775)

		cmd = self.construct_checker_job_invoker_cmd(checkerjobscript, checkerjobname)

		if self.run_jobs_flag == True:
			print("starting chado export completion checker job")
			self.run_bash(cmd)


# ================= Local utility methods ======================

	#
	# Read properties related to an apollo export files.
	# i.e. specially processed GFFs used for importing into
	# Web Apollo.
	#
	# This functionality is bolted on for Sanger and not part of the normal usage.
	#
	def read_apollo_export_configuration(self, config):

		self.apolloexport = False

		# Handle exports from Chado that will be imported to Apollo.
		# These properties are not required if the export is not Apollo related
		try:
			self.apolloconverterapp = config.get('ApolloExport', 'apollo_gff_converter_app_path').strip()

			flag = config.get('ApolloExport', 'copy_to_ftp_site_flag').strip()
			if flag == "yes":
				self.copytoftpsiteflag = True
				self.ftpsitefolder = config.get('ApolloExport', 'ftp_site_folder').strip()
				self.ftpdatedfolder = time.strftime('%Y%m%d')
				self.ftptargetfolder = self.ftpsitefolder + "/"+ self.ftpdatedfolder
			else:
				self.copytoftpsiteflag = False
				self.ftpsitefolder = ''
				self.ftptargetfolder = ''

			self.reportemailaddress = config.get('ApolloExport', 'report_email_address').strip()

			try:
				self.apolloconverterappargs = config.get('ApolloExport', 'apollo_gff_converter_app_args')
				self.apolloconverterappargs = self.apolloconverterappargs.strip()
			except:
				pass

			try:
				tmpstartdelay = config.get('ApolloExport', 'checker_job_start_delay_secs')
				self.checkerjobstartdelay = int(tmpstartdelay.strip())
			except ValueError as verr:
				raise Exception('The property checker_job_start_delay_secs is not a valid integer. Please correct the value before restarting.')
			except Exception as ex:
				pass

			self.apollogffpath = self.targetpath + "/apollo_files"
			self.apolloexport = True

		except (configparser.NoSectionError):
			pass

	#
	# Construct the shell command line string used to
	# invoke the Apollo gff polypeptide converter program.
	# The program can accept gzipped or non-gzip input.
	#
	def construct_apollo_converter_app_cmds(self, inputfile, outputfile):

		cmd = "if [[ -s \"" + inputfile + "\" ]]; then\n"
		cmd = cmd + "	set -o pipefail\n"
		cmd = cmd + "	gzip -d -c " + inputfile + " | " + self.apolloconverterapp + " " + self.apolloconverterappargs + " | gzip > " + outputfile + "\n"
		cmd = cmd + "	status=$?\n"
		cmd = cmd + "	if [[ $status -ne 0 ]]; then\n"
		cmd = cmd + "		echo \"ERROR: " + self.apolloconverterapp + " processing failed with status $status.\" 1>&2\n"
		cmd = cmd + "		rm -f " + outputfile + "\n"
		cmd = cmd + "		JOB_ERROR_STATUS=1\n"
		cmd = cmd + "	fi\n"
		cmd = cmd + "else\n"
		cmd = cmd + "	echo \"WARNING: " + inputfile + " does not exist or is empty\"\n"
		cmd = cmd + "fi\n"

		return cmd

	#
	# Utility method to create the completion checker LSF job command string.
	# To be run when all export jobs have finished.
	# Uses a wildcard to match job names.
	#
	def construct_checker_job_invoker_cmd(self, jobscriptpath, name):

		condition = "ended(" + self.jobtitle + "*)"

		cmd = "source /etc/bashrc; bsub -J " + name + " -q " + self.queue + \
		         " -R 'select[mem>3500] rusage[mem=3500] span[hosts=1]' -M 3500 " + \
		          "-o " + self.logpath + "/" + name  + ".o " + \
		          "-e " + self.logpath + "/" + name + ".e " + \
		          "-w \'" + condition + "' " + \
		          str(jobscriptpath)

		return cmd

	#
	# Utility method to write the completion checker job
	# bash script, to a given stream.
	#
	def write_checker_job_script(self, outstream, donefiles, errorlogs):

		outstream.write("#!/bin/bash\n")
		outstream.write("MAILMSG=\n")

		for donefile in donefiles:
			outstream.write("if [[ ! -f " + donefile + " ]]; then MAILMSG=${MAILMSG}'ERROR: Cannot find Chado export job completion file: " + donefile + "\\n'; fi\n")

		for errorlog in errorlogs:
			outstream.write("if [[ -s " + errorlog + " ]]; then MAILMSG=${MAILMSG}'ERROR: Errors detected in log file: " + errorlog + "\\n'; fi\n")

		outstream.write( "if [[ \"$MAILMSG\" == \"\" ]]; then echo \"Organism data has been exported to gff files.\" | mailx -s 'Chado export job [" + self.jobtitle + "] completed successfully' " + self.reportemailaddress + "; fi\n" )
		outstream.write( "if [[ \"$MAILMSG\" != \"\" ]]; then echo -e $MAILMSG | mailx -s 'Chado export job [" + self.jobtitle + "] has errors - investigation required' " + self.reportemailaddress + "; fi\n" )


# ================= Run it =====================================

if __name__ == '__main__':
	exporter = ChadoGffExporter(sys.argv)
	exporter.run()
