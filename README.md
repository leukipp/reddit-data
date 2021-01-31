# Anaconda

## Create
```
conda create -n wsbyolo python=3.8
conda install -c conda-forge pandas
conda install -c conda-forge altair
conda install -c conda-forge tqdm
conda install -c conda-forge praw
pip install simple-crypt
```

## Export
```
conda env export --no-builds -n wsbyolo > environment.yml
```

# Import
```
conda env create -f environment.yml
conda activate wsbyolo
```