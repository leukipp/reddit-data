eval "$(conda shell.bash hook)"
conda activate reddit

export $(xargs < .env)
rm -f .disabled

nohup python data.py > /var/log/reddit.log 2>&1 &