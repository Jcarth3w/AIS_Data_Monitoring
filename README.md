# AIS_Data_Monitoring_Final_Project
Final project for CS418. Monitors and stores AIS data from ships

###Change the values in the config.ini file to update user credentials for database connection###

In order to first load the database, ensure you have the "Datastore.mysql" file in your working directory.

Then, depending on your Operating System, run the "load_datastore_unix.sh" for MAC/Linux/Unix devices 
and "load_datastore_windows.ps1" for windows devices from the "Scripts directory".

After loading the database, load the data into the database. Ensure you have the "DataCSVFiles" in your working directory

Then, depending on your Operating System, run the "load_data_into_datastore_unix.sh" for MAC/Linux/Unix devices 
and "load_data_into_datastore_win.sh for windows devices from the "Scripts directory".

Once you have data loaded you are ready to run the tests.

####BEFORE RUNNING THE TESTS####
Ensure the year on your system clock is set to 2019

The reason for this is because of the inserted AIS_MESSAGES. 
The timestamps in this data are from 2020 therefore are deleted when the test for delete_msg is run

THE TESTS WILL NOT PASS IF YOUR SYSTEM CLOCK IS NOT CHANGED

If the clock is not changed all the AIS data will be deleted and queries will result in empty sets


After you have changed your system clock you are ready to run DAO.py 



