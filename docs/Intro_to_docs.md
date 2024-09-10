# Intro to docs (read this first)

## What is this documentation for?

This documentation is designed to provide guidance to developers looking to understand and replicate the code in this repo for transforming data uploaded by multiple data controllers (here assumed to be local authorities) into a compiled set of outputs for a data processor (here assumed to be a regional body tasked with analysing the data across all controllers).

## Some key terminology

* Pipeline: A pipeline is the sequence of processes (called jobs in Dagster) that take input data and produce outputs. We associate a pipeline with a dataset e.g. the SSDA903 dataset used by Children's Services to record episodes of care for looked after children. So the SSDA903 pipeline includes all the processes needed to take each individual SSDA903 file uploaded by each data controller and produce all of the outputs that are shared with the data processor. The outputs may include 'cleaned' data files that may have had fields removed, degraded and validated; log files that describe the processig that has taken place; and data models designed to plug directly into reporting software like Power BI. It's important to recognise that a pipeline may produce outputs for more than one use case (see below). For example, the SSDA903 pipeline produces outputs for two use cases: "PAN" and "SUFFICIENCY".

* Use case: A use case refers to a use of the data sharing infrastructure for a defined purpose that has associated Information Governance agreements, such as a Data Processing Agreement (DSA) and Data Protection Impact Assessment (DPIA). Use cases are agreed between the data controllers and the data processor. They define the nature of the outputs that should be produced by the pipelines and what the outputs can be used for. Note that a use case may involve more than one pipeline. For example, the "PAN" use case defines outputs to be produced for the SSDA903, CIN Census and Annex A pipelines.

## Some important foundations

* Data sharing infrastructure: The pipelines have been written to be run on cloud infrastructure that can be accessed by both the data controllers, who upload input data to the infrastructure, and the data processor, who retrieves output data from the infrastructure. The pipelines assume that the infrastructure has already managed the upload of data, has separated that data into distinct areas belonging to each data controller, and has labelled those areas in such a way that the controller can be identified by the pipeline code. When the pipeline code begins, it inherits from the infrastructure's scheduler the necessary information about the location of the input file to be processed and the identity of the data controller.

* Schema-based validation and processing: A key foundation for the pipelines is the schema. This describes two fundamental aspects of the processing:
    * the structure of the input datasets, including:
        * the fields
        * formats of data fields
        * values allowed in categorical fields
        * whether field values are mandatory
    * the instructions on how to transform data to produce the outputs, including:
        * whether files, or fields within files, should be retained for a specific output
        * whether fields should be degraded e.g. reducing the granularity of geographical locators
        * how long files should be retained before being deleted
The schema for a dataset should correspond exactly to the processing detailed in the Information Governance for the use cases relevant to that dataset. An audit of the schema should be able to map every data transformation and decision to an instruction detailed in the Information Governance.