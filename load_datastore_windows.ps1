$username = Read-Host "Please enter mysql username: "
Get-Content datastore.mysql | mysql -u $username -p
python.exe load_data_into_datastore.py