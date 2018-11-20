# Chado Export
Export sequences from the CHADO database to GFF file.

[![Build Status](https://travis-ci.org/sanger-pathogens/chado-export.svg?branch=master)](https://travis-ci.org/sanger-pathogens/chado-export)   
[![License: GPL v3](https://img.shields.io/badge/License-GPL%20v3-brightgreen.svg)](https://github.com/sanger-pathogens/chado-export/blob/master/LICENSE.txt)   

## Contents
  * [Introduction](#introduction)
  * [Installation](#installation)
    * [Required dependencies](#required-dependencies)
    * [From Source](#from-source)
    * [Running the tests](#running-the-tests)
  * [Usage](#usage)
  * [License](#license)
  * [Feedback/Issues](#feedbackissues)

## Introduction
A script to export all or a specific set of organism sequences from the CHADO database to GFF file.

Sanger Institute staff should refer to the [wiki](http://mediawiki.internal.sanger.ac.uk/index.php?title=Pathogens_GFF3_Export) for further information.

## Installation
Chado Export has the following dependencies:

### Required dependencies
* psycopg2

Details for installing Chado Export are provided below. If you encounter an issue when installing Chado Export please contact your local system administrator. If you encounter a bug please log it [here](https://github.com/sanger-pathogens/chado-export/issues) or email us at path-help@sanger.ac.uk.

### From Source
Download the latest release from this github repository or clone it. Run the tests:

    python3 setup.py test

If the tests all pass, install:

    python3 setup.py install

### Running the tests
The test can be run from the top level directory:

    python3 setup.py test

## Usage
```
usage: /usr/local/bin/generate_gff_from_chado.py [-h] -i CONFIGFILE [-a]
                                                 [-f ORG_LIST_FILE]

Script to export Chado database organism data to GFF files.

optional arguments:
  -h, --help        show this help message and exit
  -i CONFIGFILE     Path of script configuration file
  -a                Export all public Chado organisms to GFF (overrides -f
                    option)
  -f ORG_LIST_FILE  A file containing a custom list of organisms to export
                    from Chado
```
## License
Chado Export is free software, licensed under [GPLv3](https://github.com/sanger-pathogens/chado-export/blob/master/LICENSE.txt).

## Feedback/Issues
Please report any issues to the [issues page](https://github.com/sanger-pathogens/chado-export/issues) or email path-help@sanger.ac.uk.
