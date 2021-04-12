eval "$(conda shell.bash hook)"
conda activate reddit

rm -f .disabled
nohup python data.py --publish 7200 > /var/log/reddit.log 2>&1 &