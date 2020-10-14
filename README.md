- configuring new ubuntu vm instance
sudo apt-get update
sudo apt install git
git config --global user.email "--email--"
git config --global user.name "--name--"

sudo apt install python3-pip
pip3 install --upgrade pip
sudo -s ln /usr/bin/python3 /usr/bin/python
sudo -s ln /usr/bin/pip3 /usr/bin/pip
git clone 
manually create folders /logs, /data/, /db
transfer _myconfig.py 
pip install -r requirements.txt


	IDEAS
- short again up to 3 times if drawback -50% and trend on same direction(only for shorts)
- record max change (later to be used to exit trade, setting trailing stop)
	
	TODO
- report of n worst performers, n top performers among current positions
- daily snapshot
- daily backup of db to dropbox, overwrite existing
- daily backup of prev day logs, append
- telegram notification of main events
