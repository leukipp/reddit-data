# Reddit - Finance Posts
Reddit submissions from finance/investment/stock related posts:
- [r/wallstreetbets](https://reddit.com/r/wallstreetbets)
- [r/gme](https://reddit.com/r/gme)
- [r/stocks](https://reddit.com/r/stocks)
- [r/pennystocks](https://reddit.com/r/pennystocks)
- [r/robinhoodpennystocks](https://reddit.com/r/robinhoodpennystocks)
- [r/investing](https://reddit.com/r/investing)
- [r/stockmarket](https://reddit.com/r/stockmarket)
- [r/robinhood](https://reddit.com/r/robinhood)
- [r/personalfinance](https://reddit.com/r/personalfinance)
- [r/options](https://reddit.com/r/options)
- [r/forex](https://reddit.com/r/forex)
- [r/financialindependence](https://reddit.com/r/financialindependence)
- [r/finance](https://reddit.com/r/finance)
- [r/securityanalysis](https://reddit.com/r/securityanalysis)

## Requirements

### Anaconda
```sh
conda env create -f environment.yml
conda activate reddit
```

## Usage
Use the dataset available at [Kaggle](https://www.kaggle.com/leukipp/reddit-finance-data) or generate your own one:

### Environment
Create file `.streamlit/secrets.toml` and set environment variables.
```sh
KAGGLE_USERNAME="KAGGLE_USERNAME"
KAGGLE_KEY="KAGGLE_KEY"
USER_AGENT="USER_AGENT"
```

### Generate
```sh
python data.py --config config/wallstreetbets.json
```
or
```sh
python loader/pushshift.py --config config/wallstreetbets.json
python loader/crawler.py --config config/wallstreetbets.json
python loader/reddit.py --config config/wallstreetbets.json
```

### Stop
```sh
touch .disabled
```

```sh
rm .disabled
```

## License
[MIT](https://github.com/leukipp/RedditFinanceData/blob/master/LICENSE.md)