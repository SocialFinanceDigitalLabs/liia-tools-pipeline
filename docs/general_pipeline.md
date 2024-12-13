# General Pipeline

This general outline documents the steps that most of these pipelines will follow. For some pipelines, some steps may be skipped or additional steps may be added, however, by sticking to these general steps we can ensure that the data is processed in a consistent way, as well as making it easier to maintain the codebase

The high-level steps can be summarised as follows:

1. Prep data - move file and collect metadata
2. Clean data - ensure file is in a consistent format
3. Enrich data - add additional data from other sources, e.g. LA or year
4. Apply Privacy Policy - degrade data to meet data minimisation rules
5. History - sessions data
6. Concatenate data - concatenate data for each LA
7. Prepare reports

But we will go into more detail below.

For steps 1-6, there will be:

* an 'input' file area, where the files are uploaded to.
* a 'workspace' file area, containing 'current' and 'sessions' folders. 
  * the 'current' folder contains a copy of the processed data appropriately cleaned and minimised. 
  * the 'sessions' folder contains a history of each session, including the incoming, cleaned, enriched and degraded files as well as an error report. 
  * these folders are only visible to the pipeline but can be accessed by technical staff in case of troubleshooting. 
* a 'shared' file area, containing 'current', 'concatenated' and 'error_report' folders.
  * the 'current' folder contains a copy of the data from the 'workspace/current' folder.
  * the 'concatenated' folder contains the concatenated data produced in step 6.
  * the 'error_report' folder contains a copy of the error report from the 'input/sessions' folder.
  * this folder can be accessed by central pipelines for creating reports.

For step 7, there will be:

* an 'input' file area, which will be the previous steps' 'shared/concatenated' folder.
* a 'workspace' file area, containing 'current' and 'sessions' folders. 
  * the 'current' folder contains a copy of the reports created for each use case. 
  * the 'sessions' folder contains a history of each session, including the incoming files. 
  * these folders are only visible to the pipeline but can be accessed by technical staff in case of troubleshooting. 
* a 'shared' file area, containing a copy of all files to be shared with the Organisation and an 'error_report' folder.
  * the 'error_report' folder contains a copy of the error report from the 'input/sessions' folder from the previous steps.
  * this folder can seen by the Organisation account, so any files in here can be downloaded by the regional hub users.

## Prep data

Initial setup & configuration including creating a new session folder, moving the incoming file to the session folder, and collecting metadata.

Expect to be a standard task to cover all pipelines.

If no new files are found, this step simply exits.

Returns:

* Session folder
* List of files
* Metadata - if provided in incoming folder, such as folder name

## Clean data

* Detects the year.
* Checks the year is within the retention policy.
* Reads and parses the incoming files.
* Ensures that the data is in a format consistent with the schema and that all required fields are present. 
* Collects "error" information of any quality problems identified such as:

  * File older than retention policy
  * Unknown files i.e. cannot be matched against any in the schema
  * Blank files
  * Missing headers
  * Missing fields
  * Unknown fields
  * Incorrectly formatted data / categories
  * Missing data

* Creates dataframes for the identified tables
* Applies retention policy to dataframes, including file names, headers and year e.g. a file (or a column within a file) that is not used in any outputs that the regional hub is permitted to access will not be processed.

Inputs:

  * Session folder
  * Incoming files

Outputs:

  * Dataframes for retained tables
  * Error report
  * Relevant metadata

## Enrich data

Adds standard enrichments to the data, these include:

  * Adds suffix to ID fields to ensure uniqueness
  * Adds LA name
  * Adds detected year

Tables and columns names can be provided through configuration. Other functions can be added to the enrichment pipeline as required.

Inputs:

  * Dataframes for each table
  * Metadata

Outputs:

  * Dataframes for each table

## Apply Privacy Policy

Removes sensitive columns and data, or masks / blanks / degrades the data to meet data minimisation rules.

Working on each of the tables in turn, this process will degrade the data to meet data minimisation rules that should be specified in the processing instructions received. Examples of this include:

  * Dates of birth all set to the first of the month
  * Postcodes all set to the first 4 characters (excluding spaces)

Inputs:

  * Dataframes for each table

Outputs:

  * Dataframes for each table

## History - sessions data

Creates a historical archive of the data after each processing step into a unique session_id folder. This includes a subfolder for:

* Incoming data
* Cleaned data
* Enriched data
* Degraded data - privacy policy applied

The session_id folder also contains an error report detailing the errors for all files processed during the session.

This process is structured so that an archive of the historical data is preserved in case of data corruption, and should allows the steps to be re-run to build as much
history as is retained.

## Concatenate data

Concatenates the data of multiple years into a single dataframe for each LA and file type. For example, six years of SSDA903 episodes files for one LA are concatenated together. During this process data is also deduplicated using the unique_key and sort arguments in the pipeline schemas.

## Prepare reports

Use the concatenated data to create reports to be shared. These can vary from a further concatenated dataset, combining multiple LAs data, to specific analytical outputs built around several datasets.