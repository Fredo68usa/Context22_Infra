# Context22_Infra
Scripts to manage and monitor easily a Guardium infrastructure.

The purpose of this repositories is to process STAP status logs provided by Guardium to detect the STAP agents in need of attention. 
An STAP can be considered as down or disconnected when it has not reported to a collector for sometimes.
In Guardium this "sometimes" is 3 mminutes and the STAP is flagged Red in the GUI.
We consider this period as not meaningful because too short and has also been experienced as buggy with 2 staps reporting at 
exactly the same time with one green and one red.
We prefer to look at the amount of time since last time the agent reported.

The most important script is StapsDown_OO.py

It assumes the Guardium STAP logs have been upoaded into SonarG or GBDI. We have not converted this to ElasticSearch at this time.
The program assumes you provided an inventory of the agents in order for the script to detect the missing agents as well. The program also 
uses a PostGresql database where the stap down staps are listed. This is due to the fact that updates are better done into an RDBMS as well as 
managing primary keys.

Once the program has run, the postgresql DB can be queries with the .sh scripts calling a sql script not provided here (a mistake easily fixable)

You are here authorzed to edit the python program to put in the usernames and passwords you may need to access SonarG and postgresql.

