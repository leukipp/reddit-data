# Anaconda

## Create
```
conda create -n wsbyolo python=3.8
conda install -c conda-forge matplotlib 
conda install -c conda-forge pytables
conda install -c conda-forge pandas
conda install -c conda-forge altair
conda install -c conda-forge praw
conda install -c conda-forge lxml
conda install -c conda-forge tqdm
pip install simple-crypt
pip install streamlit
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