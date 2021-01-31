# Anaconda
## Create
```
conda create -n wsbyolo python=3.8
```

## Export
```
conda env export --from-history > environment.yml
```

# Import
```
conda env create -f environment.yml
conda activate wsbyolo
```