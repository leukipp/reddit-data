# Reddit Data
Download submissions from selected subreddits.
The data is exported as `.csv` file, all times in *UTC*:
| Column                  | Description                                                    | Type     |
| ----------------------- | -------------------------------------------------------------- | -------- |
| `submission`            | The id of the submission                                       | *string* |
| `subreddit`             | The subreddit name                                             | *string* |
| `author`                | The redditors username                                         | *string* |
| `created`               | Time the submission was created                                | *number* |
| `retrieved`             | Time the submission was retrieved                              | *number* |
| `edited`                | Time the submission was modified                               | *number* |
| `pinned`                | Whether or not the submission is pinned                        | *number* |
| `archived`              | Whether or not the submission is archived                      | *number* |
| `locked`                | Whether or not the submission is locked                        | *number* |
| `removed`               | Whether or not the submission is mod removed                   | *number* |
| `deleted`               | Whether or not the submission is user deleted                  | *number* |
| `is_self`               | Whether or not the submission is a text                        | *number* |
| `is_video`              | Whether or not the submission is a video                       | *number* |
| `is_original_content`   | Whether or not the submission has been set as original content | *number* |
| `title`                 | The title of the submission                                    | *string* |
| `link_flair_text`       | The submission link flairs text content                        | *string* |
| `upvote_ratio`          | The percentage of upvotes from all votes on the submission     | *number* |
| `score`                 | The number of upvotes for the submission                       | *number* |
| `gilded`                | The number of gilded awards on the submission                  | *number* |
| `total_awards_received` | The number of awards on the submission                         | *number* |
| `num_comments`          | The number of comments on the submission                       | *number* |
| `num_crossposts`        | The number of crossposts on the submission                     | *number* |
| `selftext`              | The submission selftext on text posts                          | *string* |
| `thumbnail`             | The submission thumbnail on image posts                        | *string* |
| `shortlink`             | The submission short url                                       | *string* |

## Requirements

### Packages
```bash
sudo apt install libsnappy-dev
```

```bash
pip3 install -r requirements.txt
```

### Environment
Create file `.streamlit/secrets.toml` and set environment variables:
```ini
# application
USER_AGENT="python:https://github.com/[USER]/[REPOSITORY]"

# reddit api
REDDIT_CLIENT_ID="[...]"
REDDIT_CLIENT_SECRET="[...]"

# kaggle api (optional)
KAGGLE_USERNAME="[...]"
KAGGLE_KEY="[...]"
```

Kaggle is only needed if you want to upload your dataset periodically. In that case, you need to create a `config/kaggle.json` file, similar to the [dataset-metadata.json](https://github.com/Kaggle/kaggle-api/wiki/Dataset-Metadata) file.

## Run

Adjust the start time (unix timestamp) in `config/loader.json` and run:
```bash
python3 data.py <subreddit1> <subreddit2> <subreddit3> ...
```

## Download

Feel free to download some of the existing datasets available on [Kaggle](https://www.kaggle.com/leukipp/datasets?search=reddit) as well.

## License
[MIT](/LICENSE)