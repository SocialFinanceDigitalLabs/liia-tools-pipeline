# Children's Services' Data Tool
![All Tests](https://github.com/SocialFinanceDigitalLabs/liia-tools-pipeline/actions/workflows/tests.yml/badge.svg)
[![codecov](https://codecov.io/github/SocialFinanceDigitalLabs/liia-tools-pipeline/graph/badge.svg?token=R1YSMXDX1B)](https://codecov.io/github/SocialFinanceDigitalLabs/liia-tools-pipeline)

This repository holds a set of tools and utilities for processing and cleaning Children's Services' data.

## Introduction to LIIA project 

The LIIA (London Innovation and Improvement Alliance) project brings together Children’s Services data from all the 
Local Authorities (LAs) in London with the aim of providing analytical insights that are uniquely possible using 
pan-London datasets. 

 
Please see [LIIA Child Level Data Project](https://liia.london/liia-programme/targeted-work/child-level-data-project) 
for more information about the project, its aims and partners. 

## Purpose of liia-tools-pipeline package 

The package is designed to process data deposited onto the data platform by local authorities such that it can be used for analysis purposes.

This is a Dagster code server library which is setup to be used as a code server.

## How to use:

### Local Development
1. Run `poetry install` (see https://github.com/SocialFinanceDigitalLabs/digi-docs/blob/main/docs/Coding-Standards/Standard-Setup.md for our standard setup guidance)
2. Copy `.env.sample` to `.env` and fill in the variables there as needed
For the pipeline environment variables, the user needs a folder directory that mimics the directory structure in the cloud. The env variables allow the pipeline to access this folder structure.
This structure should be followed:
pipeline_mock/
├─ la_input/
│  ├─ la_code (e.g. TT1)/
│  │  ├─ annex_a/
│  │  │  ├─ annex_a_2024_JAN.xlsx
│  │  │  ├─ annex_a_2024_JAN.xlsx
├─ first_output/
│  ├─ shared/
│  ├─ workspace/
├─ second_output/
│  ├─ shared/
│  ├─ workspace/
├─ external/
│  ├─ Ofsted/
See below for some test data to use to enable basic running of the pipelines.
3. Run the following command:
   * For LA-level pipeline work: `poetry run dagster dev -f .\liiatools_pipeline\repository_la.py`
   * For Region-level (Organisation) pipeline work: `poetry run dagster dev -f .\liiatools_pipeline\repository_org.py`
4. Once running, navigate to http://localhost:3000/
5. Add the pre-commit hook by running `pre-commit install`. This will ensure your code is formatted before you commit something.

### Sample data to test pipelines
You can find a sample file or two for each dataset in the following location of this repo:
├─ liiatools/
│  ├─ dataset_pipeline (e.g. annex_a_pipeline)/
│  │  ├─ spec/
│  │  │  ├─ samples/
e.g. liiatools\annex_a_pipeline\spec\samples\Annex_A_2024_Jan.xlsx

Data required for the external folder is located in the external_data/ folder at the root of the repository.

### Preparation for Production or Staging
How this will run in production is that the library will be brought into a docker container
with configuration specified in the file `Dockerfile_user_code`.  Which code servers are used can
be specified in the installation.
See [The SFDATA Platform's Workspace definition for details](https://github.com/SocialFinanceDigitalLabs/sfdata-platform/blob/main/dagster/workspace.yaml)

The idea is each code server will have its own setup which will be a copy of what's here.

Note: Multiple libraries, pipelines, etc can exist in a single code server. Different servers should
be used if they have conflicting requirements (e.g. different python versions)

### Documentation
Take a look at the documentation to understand what this code is designed to do and how to replicate it for your own dataset transformations.
We recommend reading [text](docs/Intro_to_docs.md) first, followed by [text](docs/general_pipeline.md).
