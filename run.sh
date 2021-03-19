eval "$(conda shell.bash hook)"
conda activate reddit

source .env
export $(grep --regexp ^[A-Z] .env | cut -d= -f1)

rm -f .disabled
nohup python data.py --publish 7200 > /var/log/reddit.log 2>&1 &