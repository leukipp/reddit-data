# Anaconda

## Import
```
conda env create -f environment.yml
conda activate reddit
```

# Usage

## Run
```
export $(xargs < .env)
```

### Full
```
python data.py --config config/wallstreetbets.json
```

### Single
```
python loader/pushshift.py --config config/wallstreetbets.json
python loader/crawler.py --config config/wallstreetbets.json
python loader/reddit.py --config config/wallstreetbets.json
```

## Disable
```
touch .disabled
```

## Enable
```
rm .disabled
```
