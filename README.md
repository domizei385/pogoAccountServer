# pogoAccountServer

Serve PTC accounts for Pokemon Go things - created to be used with the [mp-accountServerConnector](https://github.com/crhbetz/mp-accountServerConnector) MAD plugin to serve multiple separate instances of MAD from a common pool of PTC accounts.

# Setup

As I'm very short on time right now, I can't provide a proper guide. The server should run in your regular MAD python environment.

* create a new MySQL database to use
* apply `sql/accounts.sql` to the new database
* `cp config.ini.example config.ini` and customize `config.ini` with your data
* install requirements `pip install -r requirements.txt` into a python environment of your choice
* create a file `accounts.txt` that contains your PTC accounts, one per line, in the format `username,password`
* run `server.py` with your suitable `python` binary, for example `python server.py`
* setup the [mp-accountServerConnector](https://github.com/crhbetz/mp-accountServerConnector) MAD plugin for MAD to pull PTC accounts from this server

# Security

This server serves a username, password combination on request. It's a proof-of-concept type project, I can't vouch for any type of data security. I strongly disagree exposing this service to the open web at all.

# Account Management

The server will always serve the account that has been last served the longest time ago and is not currently assigned to a device,
thus continously cycling through all accounts available. It will not serve accounts released from a device less than 24h (configurable as `cooldown_hours`) ago to mitigate
the recent "maintenance screen issue" on PTC scanner accounts.

# Future development

I'm planning on integrating this server+plugin solution into the MAD account management that's currently under development, soon after that's finished.
