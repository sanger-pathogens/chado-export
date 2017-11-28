#!/usr/bin/env python3

import sys
import psycopg2
import os
import subprocess
import configparser
import argparse
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
	
	
	#
	# Top-level function to run the export
	#
	def run(self):
	
		self.read_program_arguments(self.prog_args)
		self.validate_arguments()
		self.read_configuration()
		#self.display_configuration()
		self.create_folder_structure()
		self.execute_export()
		
		
	#
	# Get command line arguments.
	#
	def read_program_arguments(self, prog_args):

		parser = argparse.ArgumentParser(prog=prog_args[0], description='Script to export Chado database organism data to GFF files.')
		parser.add_argument('-i', help='Path of script configuration file', required=False, dest='configfile', default=pkg_resources.resource_filename(__name__, 'generate_gff_from_chado.ini'))
		parser.add_argument('-a', help='Export all public Chado organisms to GFF', required=False, action='store_true', dest='dump_all')
		parser.add_argument('-f', help='A file containing a custom list of organisms to export from Chado', required=False, dest='org_list_file', default='generate_gff_from_chado.orglist')
		
		args = parser.parse_args(prog_args[1:])
		self.configfile=args.configfile.strip()
		self.dump_all=args.dump_all
		self.org_list_file=args.org_list_file.strip()

	
	#
	# Validate the program arguments
	#
	def validate_arguments(self):
	
		if len(self.configfile) == 0 or (len(self.configfile) > 0 and not os.path.isfile(self.configfile)):
			print('Configuration file not found: %s' % (self.configfile))
			exit(1)
			
	#
	# Read configuration from config file.
	#
	def read_configuration(self):
	
		config = configparser.ConfigParser()
		self.config = config
		
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
		
		
	#
	# Print out the configuration
	#
	def display_configuration(self):
	
		print("dump_all property: %s" % (self.dump_all))
		print("gtbin property: %s" % (self.gtbin))
		print("writedbentrypath property: %s" % (self.writedbentrypath))
		print("org_list_file property: %s" % (self.org_list_file))
		print("slice_size property: %d" % (self.slice_size))
		print("queue property: %s" % (self.queue))
		print("targetpath property: %s" % (self.targetpath))
		print("finalresultpath property: %s" % (self.finalresultpath))
		print("scriptpath property: %s" % (self.scriptpath))
		print("logpath property: %s" % (self.logpath))
		print("statuspath property: %s" % (self.statuspath))
		print("resultbasepath property: %s" % (self.resultbasepath))
		print("dbname property: %s" % (self.config.get('Connection', 'database')))
		print("user property: %s" % (self.config.get('Connection', 'user')))
		print("host property: %s" % (self.config.get('Connection', 'host')))
		print("password property: %s" % (self.config.get('Connection', 'password')))
		print("port property: %s" % (self.config.get('Connection', 'port')))
	
	
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
		for root, dirs, files in os.walk(self.logpath, topdown=False):
			for name in files:
				os.unlink(os.path.join(root, name))


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
	#
	def execute_export(self):
	
		jobs = []
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

			for org in sl:
				orgpath = self.resultbasepath + "/" + org
				tf.write("rm -rf " + orgpath + "\n")

			tf.write(runstr + "\n")

			for org in sl:
				orgpath = self.resultbasepath + "/" + org
				# flatten directory structure
				tf.write("find " + orgpath + " -type f " + \
		                "-exec mv {} " + orgpath + " \\;\n")
				# clean up logs
				tf.write("rm -f " + orgpath + "/tidylog.log\n")
				# remove empty dirs
				tf.write("rmdir --ignore-fail-on-non-empty " + orgpath + "/?\n")
				# merge GFFs into one file per organism
				tf.write("GT_RETAINIDS=yes " + self.gtbin + " gff3 -sort -tidy -force -retainids -o " + \
		                orgpath + "/" + org + ".gff3.gz -gzip " + orgpath + "/*.gff.gz 2> " + orgpath + "/" + org + ".tidylog \n")
				# allow access to pathdev members
				tf.write("chmod -R 775 " + orgpath + "\n")
				# move result to separate directory
				tf.write("cp " + orgpath + "/" + org + ".gff3.gz " + self.finalresultpath + "\n")
				tf.write("cp " + orgpath + "/" + org + ".tidylog " + self.finalresultpath + "\n")
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
				# make GAFs
				tf.write("get_GO_association.pl -type transcript -o " + org + " > " + self.finalresultpath + "/" + org + ".gaf \n")
				tf.write("\n")

			tf.write("touch " + self.statuspath + "/" + scriptname + ".done\n")
			tf.close()
			os.chmod(self.scriptpath + "/" + scriptname, 0o775)

			#submit script to LSF
			execline = "source /etc/bashrc; bsub -J chadoexport" + str(i) + " -q " + self.queue + " -n2  " + \
		              "-R 'select[mem>3500] rusage[mem=3500] span[hosts=1]' -M 3500 " + \
		              "-o " + self.logpath + "/" + scriptname  + ".o " + \
		              "-e " + self.logpath + "/" + scriptname + ".e " + \
		              str(self.scriptpath) + "/" + str(scriptname)
			jobs.append("chado-export-job%d" % i)

			if self.run_jobs_flag == True:
				print("starting job %d -- %s" % (i, scriptname))
				self.run_bash(execline)

# ================= Run it =====================================

if __name__ == '__main__':
	exporter = ChadoGffExporter(sys.argv)
	exporter.run()

