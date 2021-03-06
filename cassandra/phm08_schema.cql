// ***************************************************************************
// phm08_schema.cql
// https://github.com/DigiLog-N/DigiLog-N
// Copyright 2020 Canvass Labs, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ***************************************************************************
//CREATE KEYSPACE digilog_n

//Use SimpleStrategy, at least to start with.
//It's standard practice to have a replication factor of 3.
//Because we are using a single-node cluster however, a write could fail if it
// needs to successfully write to three nodes before returning.
//WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
//Note: durable_writes (writing data to commitlog first) is true by default.

USE digilog_n;


DROP TABLE phm08_train;
DROP TABLE phm08_test;
DROP TABLE phm08_live;


//Used to archive, access the original training data.
CREATE TABLE phm08_train(
	//units are not named, only numbered
	unit int,
	//timestamps are not provided, only cycles (presumably take-off + flight + landing = one engine cycle)
	cycle int,
	op1 float,
	op2 float,
	op3 float,
	sensor01 float,
	sensor02 float,
	sensor03 float,
	sensor04 float,
	sensor05 float,
	sensor06 float,
	sensor07 float,
	sensor08 float,
	sensor09 float,
	sensor10 float,
	sensor11 float,
	sensor12 float,
	sensor13 float,
	sensor14 float,
	sensor15 float,
	sensor16 float,
	sensor17 float,
	sensor18 float,
	sensor19 float,
	sensor20 float,
	sensor21 float,
	primary key ((unit), cycle)
); 


//Used to archive, access the original test data.
CREATE TABLE phm08_test(
	//units are not named, only numbered
	unit int,
	//timestamps are not provided, only cycles (presumably take-off + flight + landing = one engine cycle)
	cycle int,
	op1 float,
	op2 float,
	op3 float,
	sensor01 float,
	sensor02 float,
	sensor03 float,
	sensor04 float,
	sensor05 float,
	sensor06 float,
	sensor07 float,
	sensor08 float,
	sensor09 float,
	sensor10 float,
	sensor11 float,
	sensor12 float,
	sensor13 float,
	sensor14 float,
	sensor15 float,
	sensor16 float,
	sensor17 float,
	sensor18 float,
	sensor19 float,
	sensor20 float,
	sensor21 float,
	primary key ((unit), cycle)
); 


//Modified version of phm08_* tables used to archive live data during demo, as it arrives.
CREATE TABLE phm08_live(
	unit int,
	cycle int,
        ct_timestamp double,
	op1 float,
	op2 float,
	op3 float,
	sensor01 float,
	sensor02 float,
	sensor03 float,
	sensor04 float,
	sensor05 float,
	sensor06 float,
	sensor07 float,
	sensor08 float,
	sensor09 float,
	sensor10 float,
	sensor11 float,
	sensor12 float,
	sensor13 float,
	sensor14 float,
	sensor15 float,
	sensor16 float,
	sensor17 float,
	sensor18 float,
	sensor19 float,
	sensor20 float,
	sensor21 float,
	primary key ((unit), cycle)
); 


//Note that these bulk import statements use the sanitized csv files generated by our script in Data/PMH08.
//See GitHub for more information.
COPY digilog_n.phm08_train (unit, cycle, op1, op2, op3, sensor01, sensor02, sensor03, sensor04, sensor05, sensor06, sensor07, sensor08, sensor09, sensor10, sensor11, sensor12, sensor13, sensor14, sensor15, sensor16, sensor17, sensor18, sensor19, sensor20, sensor21) FROM './train.csv' WITH DELIMITER=',' AND HEADER=FALSE AND NULL='';

COPY digilog_n.phm08_test (unit, cycle, op1, op2, op3, sensor01, sensor02, sensor03, sensor04, sensor05, sensor06, sensor07, sensor08, sensor09, sensor10, sensor11, sensor12, sensor13, sensor14, sensor15, sensor16, sensor17, sensor18, sensor19, sensor20, sensor21) FROM './test.csv' WITH DELIMITER=',' AND HEADER=FALSE AND NULL='';

