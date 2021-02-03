# Anaconda

## Create
```
conda create -n reddit_yolo python=3.8
conda install -c conda-forge matplotlib 
conda install -c conda-forge pytables
conda install -c conda-forge pandas
conda install -c conda-forge altair
conda install -c conda-forge praw
conda install -c conda-forge lxml
conda install -c conda-forge tqdm
pip install streamlit
pip install kaggle
```

## Export
```
conda env export --no-builds -n reddit_yolo > environment.yml
```

## Import
```
conda env create -f environment.yml
conda activate reddit_yolo
```

# Docker

## ERROR: could not find an available, non-overlapping IPv4 address pool
```
sudo service openvpn stop && sudo /etc/init.d/openvpn-boot stop
```

## Normal
```
docker build -t reddit_yolo .
docker run -it reddit_yolo
```

## Compose
```
docker-compose build
docker-compose up --build
```

# Kaggle

## Config
```
kaggle config view
```

## Dataset
```
kaggle datasets init -p data/public
kaggle datasets version -p data/public -r zip -m "update data"
```

## Status
```
kaggle datasets status leukipp/reddityolo
```

# Run

## Full
```
python data.py --global-config config/wallstreetbets.json
```

## Single
```
python loader/pushshift.py --global-config config/wallstreetbets.json
python loader/crawler.py --global-config config/wallstreetbets.json
python loader/reddit.py --global-config config/wallstreetbets.json
```