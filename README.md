# OnlineCodeDaemon
The OnlineCodeDaemon project is a part of the PDL server infrastructure. 

PDL is a standard of the International Virtual Observatory Alliance (http://ivoa.net/documents/PDL/).
It is a grammar for finely describe the set of inputs (with related constraints) and output of a generic web service. 
Since all the parameters are finely described, generic software elements can be automatically configured into ad-hoc tools using a PDL description instance as a configuration file. 

The main elements of a PDL service architecture are:
-The PDL server
-The PDL client


THE PDL SERVER 

The goal of the PDL server is to easily expose any code as a web service. 
This is a generic software written “once at all” for all the possible services. 
This generic server handle the job submission, the job queuing, the monitoring of job state, the notification to user, the lifecycle of computed results, etc. 
To adapt this generic server to a particular service, one must only provide a PDL description of the service to deploy. 


The PDL server is composed of 7 software elements. Each element corresponds to a project:

-OnlineCodeLibs —> Contains the set of Libraries needed by all the other components

-OnlineCodeCommons —> Contains the commons objects used by all the other components. 

-OnlindeCodeDAO —> Contains the set of tools for storing/reading information about jobs and users into a SQL database

-OnlineCodeBusiness  —> Contains all the logic and rules for the job/user management

-OnlineCodeServelet —> The entry point of the service for launching new jobs.

-OnlineCodeDaemon —> The server-side job management daemon

-JobMonitor —> A rich Web interface (based on Google Web Toolkit) for job monitoring (this interface works only with the contained into the “version2” branch of the PDL server). 


THE PDL CLIENT
For historical reasons the PDL generic client was developed on a google code repository : https://code.google.com/p/vo-param/
