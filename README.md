# Experimental Setup

This setup has been configured in the UMC hospital environment without access to admin rights, or possibility to install any external programs. The programs used and their source of download are outlined here:

- [PostgreSQL 12.4](https://sourceforge.net/projects/pgsqlportable/)
- [Python 3.8.12](https://github.com/winpython/winpython/releases/tag/4.6.20220501), also contains Jupyter to develop in notebooks

# Reproducing database environment

First, rename the downloaded portable Python folder to `Python`. Then, execute the [powershell script `env_start.ps1`](env_start.ps1) to launch a Jupyter environment where the data and scripts are located. The data required to execute the program needs to be saved in a `data` folder.

To access the database, run the two files `PostgreSQL-Start.bat` and `PgAdmin4.bat`. Then from the PostgreSQL web browser tab, add a server at the address `localhost`.

# Description of scripts

Both scripts are recreated in two forms, a first one which saves the required data to an SQL database, and another which ignores the database (contains `_db` in file name). The two scripts are:

- `patient_selection` script that takes care of selecting the patients for the clinic
- `bpd` script that takes care of diagnosing the type of BPD in patients