How to run this project:

i) Download the latest version of JDBC

   From: https://jdbc.postgresql.org/download.html
	
ii) Go to: project -> properties -> Java Build Path -> Libraries -> add External Jars

	There add the jar you downloaded from the link above
	
iii) Make sure you have a postgres server setup in port 5432 (otherwise specify the port in the SQLHandler class)

iv) Set your postgres credentials in the auth.json file

Also please add this repository to your build path https://github.com/kostaskougios/cloning.
