rm -fr deps
mkdir deps

python3.10 -m pip wheel --wheel-dir ./deps -r requirements.txt  

