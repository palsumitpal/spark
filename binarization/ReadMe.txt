This is a simple sample code to do data binarization of data.

Build
=====
Build the code using the build.sbt file in the root directory

sbt package

How to run the code
===================
spark-submit --class  BinarizeAllColumns <Full Path to Jar File with Jar File Name> <Full Path to Data File will Data File Name> <List of columns that needs to be binarized ( starts with 0)>

Output
======

Header has the <Col#_DistinctColValu>


Assumptions
===========
1st line is header
Field separator is '\t'
no field contains '\t'
All columns which are not binarized ( if not specified in the input arg list ) - would be the last columns appended
output is in directory <binarizedout>

Enhancements
============
- Modularize the code
- Unit Tests needs to be written
- Edge cases testing needs to be done




