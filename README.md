# Anaconda

## Create
```
conda create -n reddit python=3.8
```

## Install
```
conda install -c conda-forge matplotlib 
conda install -c conda-forge pytables
conda install -c conda-forge pandas
conda install -c conda-forge altair
conda install -c conda-forge praw
conda install -c conda-forge lxml
conda install -c conda-forge tqdm
```

```
pip install streamlit
pip install kaggle
```

## Export
```
conda env export --no-builds -n reddit > environment.yml
```

## Import
```
conda env create -f environment.yml
conda activate reddit
```

# Docker

## ERROR: could not find an available, non-overlapping IPv4 address pool
```
sudo service openvpn stop && sudo /etc/init.d/openvpn-boot stop
```

## Normal
```
docker build -t reddit .
docker run -it reddit
```

## Compose
```
docker-compose build
docker-compose up --build
```

# Azure

## Create
```
az acr create --resource-group docker-rg --name reddit --sku Basic
docker context create aci aci
```

## Login
```
docker login reddit.azurecr.io -u reddit -p pz...Vv
docker context use aci
```

## Deploy
``` 
docker-compose build && docker-compose push
docker compose up -p reddit
```

## Restart
```
az container restart --resource-group docker-rg --name reddit
docker ps
```

## Status
```
az container show --resource-group docker-rg --name reddit --query instanceView.state
az container show --resource-group docker-rg --name reddit --query ipAddress.fqdn
az container show --resource-group docker-rg --name reddit --query ipAddress.ip
```

## Stop
```
docker compose down -p reddit
``` 

## Cleanup
```
az acr run --registry reddit --cmd "acr purge --filter 'data:.*' --ago 1h --untagged" /dev/null
az acr run --registry reddit --cmd "acr purge --filter 'visualize:.*' --ago 1h --untagged" /dev/null
```

## Delete
```
az container delete --resource-group docker-rg --name reddit
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
kaggle datasets status leukipp/reddit-finance-data
```

# Run

## Full
```
python data.py --config config/wallstreetbets.json
```

## Single
```
python loader/pushshift.py --config config/wallstreetbets.json
python loader/crawler.py --config config/wallstreetbets.json
python loader/reddit.py --config config/wallstreetbets.json
```