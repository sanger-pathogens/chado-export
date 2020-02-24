#!/usr/bin/env python3

import sys
import stat
import os
import pkg_resources
import unittest
import io
import tempfile
import subprocess

from nose import SkipTest

from generate_gff_from_chado import * 
from nose.tools import assert_equal, assert_not_equal, assert_raises, assert_true

#
# NOTE: This test class relies on the contents of the test orglist file.
# If this is changed then the test will also
# need to be updated.
#
# Author: K. Pepper
#
class TestChadoGffExporter:

	DEFAULT_ORG_LIST_FILE = 'generate_gff_from_chado.orglist'
	INI_FILE = os.path.join(sys.path[0]+'/resources/test_generate_gff_from_chado.ini')
	APOLLO_INI_FILE = os.path.join(sys.path[0]+'/resources/test_generate_gff_from_chado-apollo.ini')
	ORGLIST_FILE1= 'resources/test_generate_gff_from_chado.orglist1'
	ORGLIST_FILE2= 'resources/test_generate_gff_from_chado.orglist2'
	BASE_DIR = "/tmp/chado-export"
	
	ORG_FILE1_CHUNKS = [['Bsaltans', 'Bxylophilus', 'Eacervulina', 'Ebrunetti', 'Egranulosus', 'Emaxima', 'Emitis', 'Emultilocularis', 'Enecatrix', 'Epraecox'], \
						['Etenella', 'Gpallida', 'Hmicrostoma', 'Lbraziliensis', 'Ldonovani_BPK282A1', 'Linfantum', 'Lmajor', 'Lmexicana', 'Ncaninum', 'Pberghei'], \
						['Pchabaudi', 'Pfalciparum', 'Pgallinaceum', 'Pknowlesi', 'Pmalariae', 'Povale', 'Preichenowi', 'Prelictum', 'PvivaxP01', 'Pyoelii'], \
						['Sjaponicum', 'Smansoni', 'Tannulata', 'Tbruceibrucei927', 'Tbruceigambiense', 'TbruceiLister427', 'Tcongolense', 'Tcruzi', 'Tsolium', 'Tvivax']]
						
	ORG_FILE2_CHUNKS = [['Bsaltans', 'Bxylophilus', 'Eacervulina'], \
						['Ebrunetti', 'Egranulosus', 'Emaxima'], \
						['Emitis', 'Emultilocularis', 'Enecatrix'], \
						['Epraecox']]
	
	def setup(self):
		args = ['program_name']
		self.chadoGffExporter = ChadoGffExporter(args) 

	def teardown(self):
		self.chadoGffExporter = None

	def checkAutoBuild(self):
		if os.environ.get('TRAVIS_BUILD') and os.getenv('TRAVIS_BUILD') == 'yes':
			raise SkipTest("Test skipped for automatic builds")
			
	
	def test_01_read_program_arguments1(self):

		# Given
		args = ['program_name', '-i', TestChadoGffExporter.INI_FILE]

		# When
		self.chadoGffExporter.read_program_arguments(args)

		# Then
		assert self.chadoGffExporter.apolloexport_property == False
		assert self.chadoGffExporter.configfile_property.endswith(TestChadoGffExporter.INI_FILE)
		assert self.chadoGffExporter.org_list_file_property == TestChadoGffExporter.DEFAULT_ORG_LIST_FILE
		assert self.chadoGffExporter.dump_all_property == False
		assert self.chadoGffExporter.gt_filepath_wildcard_escaping_property == False
		
		
	def test_02_read_program_arguments2(self):
		
		# Given
		args = ['program_name', '-a', '-i', TestChadoGffExporter.INI_FILE]

		# When
		self.chadoGffExporter.read_program_arguments(args)

		# Then
		assert self.chadoGffExporter.dump_all_property == True
		
	def test_03_read_program_arguments3(self):
		
		# Given
		args = ['program_name', '-i', '/somedirectory/config.ini']

		# When
		self.chadoGffExporter.read_program_arguments(args)

		# Then
		assert self.chadoGffExporter.configfile_property == '/somedirectory/config.ini'
		
	def test_04_read_program_arguments4(self):
		
		# Given
		args = ['program_name', '-i', TestChadoGffExporter.INI_FILE, '-f', '/somedirectory/organism.list']

		# When
		self.chadoGffExporter.read_program_arguments(args)

		# Then
		assert self.chadoGffExporter.org_list_file_property == '/somedirectory/organism.list'
		
	def test_05_read_program_arguments5(self):
	
		# Given
		args = ['program_name', '-a', '-i', '/somedirectory/config.ini', '-f', '/somedirectory/organism.list']

		# When
		self.chadoGffExporter.read_program_arguments(args)

		# Then
		assert self.chadoGffExporter.configfile_property == '/somedirectory/config.ini'
		assert self.chadoGffExporter.dump_all_property == True
		assert self.chadoGffExporter.org_list_file_property == '/somedirectory/organism.list'
	
	
	def test_06_read_configuration(self):

		# Given
		args = ['program_name', '-i', TestChadoGffExporter.INI_FILE]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()
		self.chadoGffExporter.display_configuration()

		# Then
		assert self.chadoGffExporter.gtbin_property == '/applications/gt'
		assert self.chadoGffExporter.writedbentrypath_property == '/applications/writedb_entry'
		assert self.chadoGffExporter.slice_size_property == 10
		assert self.chadoGffExporter.queue_property == 'basement'
		assert self.chadoGffExporter.jobtitle_property == "chadoexp"
		assert self.chadoGffExporter.targetpath_property == TestChadoGffExporter.BASE_DIR
		assert self.chadoGffExporter.finalresultpath_property == self.chadoGffExporter.targetpath_property + '/results'
		assert self.chadoGffExporter.scriptpath_property == self.chadoGffExporter.targetpath_property + '/scripts'
		assert self.chadoGffExporter.logpath_property == self.chadoGffExporter.targetpath_property + '/logs'
		assert self.chadoGffExporter.statuspath_property == self.chadoGffExporter.targetpath_property + '/status'
	
		assert self.chadoGffExporter.apolloexport_property == False
		assert len(self.chadoGffExporter.apolloconverterapp_property) == 0
		assert len(self.chadoGffExporter.apolloconverterappargs_property) == 0
		assert len(self.chadoGffExporter.ftpsitefolder_property) == 0
		assert len(self.chadoGffExporter.reportemailaddress_property) == 0
		assert self.chadoGffExporter.checkerjobstartdelay_property == 10

		assert self.chadoGffExporter.gt_filepath_wildcard_escaping_property == False

	def test_07_read_organism_list_from_file(self):
	
		# Given
		args = ['program_name', '-i', TestChadoGffExporter.INI_FILE, '-f', 'test/'+TestChadoGffExporter.ORGLIST_FILE1]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()
		organisms = self.chadoGffExporter.read_organism_list_from_file()
		
		# Then
		assert len(organisms) == 40

	# Must skip on Travis as no database connection available
	# @unittest.skipIf("TRAVIS_BUILD" in os.environ and os.environ["TRAVIS_BUILD"] == "yes", "Skipping this test on Travis CI.")
	def test_08_get_organism_list_all(self):
	
		# Hack as there's no decent annotation mechanism in nose to conditionally skip tests...
		self.checkAutoBuild()
		
		# Given
		args = ['program_name', '-a', '-i', TestChadoGffExporter.INI_FILE]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()
		
		# Then - will read from chado!
		i = 0
		for org in self.chadoGffExporter.get_organism_list(10):
			print("organism: %s" % (org))
			
			assert len(org) > 0
			
			i = i + 1

		assert i > 1


	def test_09_get_organism_list_from_file1(self):
	
		# Given
		args = ['program_name', '-i', TestChadoGffExporter.INI_FILE, '-f', 'test/'+TestChadoGffExporter.ORGLIST_FILE1]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()

		# Then
		i = 0
		for org in self.chadoGffExporter.get_organism_list(10):
		
			print("organism: %s" % (org))
			
			assert TestChadoGffExporter.ORG_FILE1_CHUNKS[i] == org
			i = i + 1

		assert i == 4
	
	def test_10_get_organism_list_from_file2(self):
	
		# Given
		args = ['program_name', '-i', TestChadoGffExporter.INI_FILE, '-f', 'test/'+TestChadoGffExporter.ORGLIST_FILE2]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()

		# Then - use different chunk size
		i = 0
		for org in self.chadoGffExporter.get_organism_list(3):
		
			print("organism: %s" % (org))
			
			assert TestChadoGffExporter.ORG_FILE2_CHUNKS[i] == org
			i = i + 1

		assert i == 4

	# @unittest.skipIf("TRAVIS_BUILD" in os.environ and os.environ["TRAVIS_BUILD"] == "yes", "Skipping this test on Travis CI.")
	def test_11a_create_folder_structure(self):
	
		# Hack as there's no decent annotation mechanism in nose to conditionally skip tests...
		self.checkAutoBuild()
			
		# Given
		args = ['program_name', '-i', TestChadoGffExporter.INI_FILE, '-f', 'test/'+TestChadoGffExporter.ORGLIST_FILE1]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()
		os.makedirs(self.chadoGffExporter.targetpath_property, exist_ok=True)
		self.chadoGffExporter.create_folder_structure()
		
		# Then
		assert os.path.isdir(self.chadoGffExporter.targetpath_property + '/results')
		assert os.path.isdir(self.chadoGffExporter.targetpath_property + '/scripts')
		assert os.path.isdir(self.chadoGffExporter.targetpath_property + '/logs')
		assert os.path.isdir(self.chadoGffExporter.targetpath_property + '/status')
		assert os.path.isdir(self.chadoGffExporter.targetpath_property + '/results')
		assert self.chadoGffExporter.apolloexport_property == False
		
	# @unittest.skipIf("TRAVIS_BUILD" in os.environ and os.environ["TRAVIS_BUILD"] == "yes", "Skipping this test on Travis CI.")
	def test_11b_create_folder_structure_apolloexport(self):
	
		# Hack as there's no decent annotation mechanism in nose to conditionally skip tests...
		self.checkAutoBuild()
			
		# Given
		args = ['program_name', '-i', TestChadoGffExporter.APOLLO_INI_FILE, '-f', 'test/'+TestChadoGffExporter.ORGLIST_FILE1]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()
		os.makedirs(self.chadoGffExporter.targetpath_property, exist_ok=True)
		#self.chadoGffExporter.validate_config()
		self.chadoGffExporter.display_configuration()
		self.chadoGffExporter.create_folder_structure()
		
		# Then
		assert self.chadoGffExporter.apolloexport_property == True
		assert os.path.isdir(self.chadoGffExporter.apollogffpath_property)
		assert os.path.isdir("/tmp/ftpsite")
	
	
	# @unittest.skipIf("TRAVIS_BUILD" in os.environ and os.environ["TRAVIS_BUILD"] == "yes", "Skipping this test on Travis CI.")
	def test_12_execute_export(self):
	
		# Hack as there's no decent annotation mechanism in nose to conditionally skip tests...
		self.checkAutoBuild()
			
		# Given
		args = ['program_name', '-i', TestChadoGffExporter.INI_FILE, '-f', 'test/'+TestChadoGffExporter.ORGLIST_FILE1]
		
		# Don't actually run the export..
		self.chadoGffExporter.run_jobs = False

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()
		self.chadoGffExporter.create_folder_structure()
		
		assert self.chadoGffExporter.run_jobs == False
		
		self.chadoGffExporter.execute_export()
		
		# Then
		# We should now have four generated files [scripts] based on the current test organism list file
		list = os.listdir(self.chadoGffExporter.scriptpath_property) # dir is your directory path
		num_scripts = len(list)
		assert num_scripts == 4

	def test_13_writeCheckerScript(self):
	
		# Given
		jobs = [ "jobscript1", "jobscript2", "jobscript3" ]
		logs = [ "/tmp/jobscript1.e", "/tmp/jobscript2.e", "/tmp/jobscript3.e" ]
		self.chadoGffExporter.reportemailaddress_property = 'person@address.com'
		self.chadoGffExporter.jobtitle_property = 'GenedbExport'
		
		expected_output = "#!/bin/bash\n" + \
						"MAILMSG=\n" + \
						"if [[ ! -f jobscript1 ]]; then MAILMSG=${MAILMSG}'ERROR: Cannot find Chado export job completion file: jobscript1\\n'; fi\n" + \
						"if [[ ! -f jobscript2 ]]; then MAILMSG=${MAILMSG}'ERROR: Cannot find Chado export job completion file: jobscript2\\n'; fi\n" + \
						"if [[ ! -f jobscript3 ]]; then MAILMSG=${MAILMSG}'ERROR: Cannot find Chado export job completion file: jobscript3\\n'; fi\n" + \
						"if [[ -s /tmp/jobscript1.e ]]; then MAILMSG=${MAILMSG}'ERROR: Errors detected in log file: /tmp/jobscript1.e\\n'; fi\n" + \
						"if [[ -s /tmp/jobscript2.e ]]; then MAILMSG=${MAILMSG}'ERROR: Errors detected in log file: /tmp/jobscript2.e\\n'; fi\n" + \
						"if [[ -s /tmp/jobscript3.e ]]; then MAILMSG=${MAILMSG}'ERROR: Errors detected in log file: /tmp/jobscript3.e\\n'; fi\n" + \
						"if [[ \"$MAILMSG\" == \"\" ]]; then echo \"Organism data has been exported to gff files.\" | mailx -s 'Chado export job [" + self.chadoGffExporter.jobtitle_property + "] completed successfully' person@address.com; fi\n" + \
						"if [[ \"$MAILMSG\" != \"\" ]]; then echo -e $MAILMSG | mailx -s 'Chado export job [" + self.chadoGffExporter.jobtitle_property + "] has errors - investigation required' " + self.chadoGffExporter.reportemailaddress_property + "; fi\n"
		
		stream = io.StringIO()
		
		# When
		self.chadoGffExporter.write_checker_job_script(stream, jobs, logs)
		contents = stream.getvalue()
		stream.close()
		
		test_assertion = "Assertion failed: \n" + expected_output + "\n\n" + contents
		
		# Then
		assert contents == expected_output, test_assertion
		
	def test_14_construct_checker_job_invoker_cmd(self):
	
		# Given
		self.chadoGffExporter.queue_property = "fastqueue"
		self.chadoGffExporter.logpath_property = "/tmp/alogpath"
		
		expected_output1 = "source /etc/bashrc; bsub -J checker-job -q fastqueue -R 'select[mem>3500] rusage[mem=3500] span[hosts=1]' " + \
						"-M 3500 -o /tmp/alogpath/checker-job.o -e /tmp/alogpath/checker-job.e -w 'ended(chadoexp*)' /tmp/checker_script.sh"
		
		# When
		cmd1 = self.chadoGffExporter.construct_checker_job_invoker_cmd("/tmp/checker_script.sh", "checker-job")
		
		# Then
		assert cmd1 == expected_output1	
	
	def test_15_read_configuration_with_apollo_export(self):

		# Given
		args = ['program_name', '-i', TestChadoGffExporter.APOLLO_INI_FILE]
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()

		# When - set properties programmatically
		

		# Then
		assert self.chadoGffExporter.apolloexport_property == True
		assert self.chadoGffExporter.apolloconverterapp_property == "script.sh"
		assert self.chadoGffExporter.apolloconverterappargs_property == "-e"
		assert self.chadoGffExporter.copytoftpsiteflag_property == True
		assert self.chadoGffExporter.ftpsitefolder_property == "/tmp/ftpsite"
		assert self.chadoGffExporter.reportemailaddress_property == "person@address.com"	
		assert self.chadoGffExporter.apollogffpath_property == self.chadoGffExporter.targetpath_property + "/apollo_files"
		assert self.chadoGffExporter.jobtitle_property == "apollojob"
		assert self.chadoGffExporter.checkerjobstartdelay_property == 20

	def test_16_validate_config_abs_paths(self):

		# Very basic validation tests using absolute paths

		gtbin = None
		writedbentrypath = None
		apolloconverterapp = None
		target_test_path = None
		ftpsitefolder_test_path = None

		try:
			# Given

			dependency_temp_dir = tempfile.TemporaryDirectory()

			try:
				gtbin = open(os.path.join(dependency_temp_dir.name, "gt-test"), "w+")
				os.chmod(gtbin.name, stat.S_IEXEC)
				writedbentry = open(os.path.join(dependency_temp_dir.name, "writedb_entry-test"), "w+")
				os.chmod(writedbentry.name, stat.S_IEXEC)
				apolloconverterapp = open(os.path.join(dependency_temp_dir.name, "gffmunger-test"), "w+")
				os.chmod(apolloconverterapp.name, stat.S_IEXEC)
			finally:
				gtbin.close()
				writedbentry.close()
				apolloconverterapp.close()

			target_test_path = tempfile.TemporaryDirectory()
			ftpsitefolder_test_path = tempfile.TemporaryDirectory()

			self.chadoGffExporter.gtbin_property = gtbin.name
			self.chadoGffExporter.writedbentrypath_property = writedbentry.name
			self.chadoGffExporter.apolloconverterapp_property = apolloconverterapp.name
			self.chadoGffExporter.targetpath_property = target_test_path.name
			self.chadoGffExporter.ftpsitefolder_property = ftpsitefolder_test_path.name

			self.chadoGffExporter.apolloexport_property = True
			self.chadoGffExporter.copytoftpsiteflag_property = True
			self.chadoGffExporter.reportemailaddress_property = 'person@address.com'
			self.chadoGffExporter.checkerjobstartdelay_property = 3

			# When/Then - should be valid (exits if not!)
			self.chadoGffExporter.validate_config()

		finally:
			dependency_temp_dir.cleanup()
			target_test_path.cleanup()
			ftpsitefolder_test_path.cleanup()

	def test_17_validate_config_sys_path(self):

		# Very basic validation tests using system path to pick up dependency programs

		gtbin = None
		writedbentrypath = None
		apolloconverterapp = None
		target_test_path = None
		ftpsitefolder_test_path = None

		try:
			# Given

			dependency_temp_dir = tempfile.TemporaryDirectory()

			try:
				gtbin = open(os.path.join(dependency_temp_dir.name, "gt-test"), "w+")
				os.chmod(gtbin.name, stat.S_IEXEC)
				writedbentry = open(os.path.join(dependency_temp_dir.name, "writedb_entry-test"), "w+")
				os.chmod(writedbentry.name, stat.S_IEXEC)
				apolloconverterapp = open(os.path.join(dependency_temp_dir.name, "gffmunger-test"), "w+")
				os.chmod(apolloconverterapp.name, stat.S_IEXEC)
			finally:
				gtbin.close()
				writedbentry.close()
				apolloconverterapp.close()

			target_test_path = tempfile.TemporaryDirectory()
			ftpsitefolder_test_path = tempfile.TemporaryDirectory()

			# Only store the dependency file names, not absolute paths for this test
			self.chadoGffExporter.gtbin_property = os.path.basename(gtbin.name)
			self.chadoGffExporter.writedbentrypath_property = os.path.basename(writedbentry.name)
			self.chadoGffExporter.apolloconverterapp_property = os.path.basename(apolloconverterapp.name)

			self.chadoGffExporter.targetpath_property = target_test_path.name
			self.chadoGffExporter.ftpsitefolder_property = ftpsitefolder_test_path.name

			# Set the dependency directory(s) in the system path
			os.environ["PATH"] = dependency_temp_dir.name

			self.chadoGffExporter.apolloexport_property = True
			self.chadoGffExporter.copytoftpsiteflag_property = True
			self.chadoGffExporter.reportemailaddress_property = "person@address.com"
			self.chadoGffExporter.checkerjobstartdelay_property = 3

			# When/Then - should be valid (exits if not!)
			self.chadoGffExporter.validate_config()

		finally:
			dependency_temp_dir.cleanup()
			target_test_path.cleanup()
			ftpsitefolder_test_path.cleanup()

	def test_18a_escape_gt_wildcards(self):

		# Test when flag is false, no escaping

		# Given
		args = ['program_name', '-a', '-i', TestChadoGffExporter.INI_FILE]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()

		# Then
		assert self.chadoGffExporter.escape_gt_wildcards(r'/*.gff.gz') == r'/*.gff.gz'

	def test_18b_escape_gt_wildcards(self):

		# Test when flag is true, so get escaping

		ini_file_flag_true = os.path.join(sys.path[0] + '/resources/test18/test_generate_gff_from_chado.ini')

		# Given
		args = ['program_name', '-a', '-i', ini_file_flag_true]

		# When
		self.chadoGffExporter.read_program_arguments(args)
		self.chadoGffExporter.read_configuration()

		# Then
		assert self.chadoGffExporter.escape_gt_wildcards(r'/*.gff.gz') == r'/\*.gff.gz'
		assert self.chadoGffExporter.escape_gt_wildcards(r'/folder1/*/*.gff.gz') == r'/folder1/\*/\*.gff.gz'
		assert self.chadoGffExporter.escape_gt_wildcards(r'/folder1/file1') == r'/folder1/file1'
		assert self.chadoGffExporter.escape_gt_wildcards(r'') == r''
