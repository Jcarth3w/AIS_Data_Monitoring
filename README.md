# AIS_Data_Monitoring_Final_Project
Final project for CS418. Monitors and stores AIS data from ships

##Use the config.ini file to update user credentials##

In order to first load the database, ensure you have the "Datastore.mysql" file in your working directory.

Then, depending on your Operating System, run the "load_datastore_unix.sh" for MAC/Linux/Unix devices and "load_datastore_windows.ps1" for windows devices from the "Scripts directory".

After loading the database, load the data into the database. Ensure you have the "DataCSVFiles" in your working directory

Then, depending on your Operating System, run the "load_data_into_datastore"