$username = Read-Host "Please enter mysql username: "
Get-Content datastore.mysql | mysql -u $username -p