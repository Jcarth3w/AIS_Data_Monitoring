# Contributors
Jack Carthew - jdcarthew@bsu.edu
Gavin Neil - grneal@bsu.edu
Dawson Vaal - dcvaal@bsu.edu
Wesley Kring - wjkring@bsu.edu

The work contained in this repository is for submission on the AIS Data Monitoring Final Project. The code contained is reflective of option A.

# AIS_Data_Monitoring_Final_Project
Final project for CS418. Monitors and stores AIS data from ships

## Instructions for running the program

* The first step is to change the values of ```username``` and ```password``` in the ```config.ini``` file to your own credentials

* Ensure that the ```Datastore.mysql``` and ```DataCSVFiles``` are in your working direcrory

* For Unix/Linux/MAC systems run ```load_datastore_unix.sh```

* For Windows select the ```load_datastore_windows.ps1``` from your ```Scripts directory```

* Once complete the data shoule be loaded and you are ready to run the tests.

## BEFORE RUNNING THE TESTS

* Ensure the year on your system clock is set to 2019

* The reason for this is because of the inserted AIS_MESSAGES contain a 2020 timestamp and will be deleted when the delete_msg test will run

* ```THE TESTS WILL NOT PASS IF YOUR SYSTEM CLOCK IS NOT CHANGED```

* The tests can then be run by running ```DAO.py```


