# psychotic-squirrel
Attribution Pipeline Orchestration

## Setup
There are a couple of steps to get setup and run the pipeline that will generate a customer attribution channel report in CSV.

### Account & API Token
- Sign-up and create an account with [ihc-attribution.com](https://login.ihc-attribution.com)
- Run the command below, then copy the API token that is provided on the overview page and save it in the `.env` file.
```bash
cp .env.example .env
```

### Environment
We can now create a virtual environment and install packages:
```bash
python3 -m venv venv && . venv/bin/activate
make install
```

### Database
The database is in a zip file and needs to be unzipped to be used:
```bash
make db-ready
```

### Test Data
Before making requests to the IHC API, we need to train the model. To create the training data set:
```bash
make sample-data-file
```

- Now navigate to the parameter training page of [ihc-attribution.com](https://login.ihc-attribution.com/training).
- Create a new conversion type, call it `test_attribution`
    - This is the default value but can be changed in `pipeline/constants.py`and updating the `CONV_TYPE_ID` value.
- Upload your training dataset (it should be saved as `data/training/training_data.json`), and submit it for training.
- After a new minutes you will recieve an email notification that you data has successfully finished training.

## Run Pipeline & Build Report
We are now ready to run our pipeline that will build the CSV report. To do this, run:
```bash
make run-pipeline
```
The pipeline will begin and submit customer journey records in chunks before creating the report.

Once the pipeline completes, the report will be saved in `data/exports/channel_reporting.csv`.

___Optional___: The pipeline can also be run for a specific date range:
```bash
make run-pipeline START_DATE=2023-08-29 END_DATE=2023-09-01
```

***
## Troubleshooting
Help menu:
```bash
make help
```

***

## Potential Enhancements
- Adding unit and integration tests to validate the logic
- Using Airflow's TaskFlow API:
    - Create and configure a dag with the `@dag` decorator
    - Reuse and decorate functions with `@task`
    - Make modifications to functions and handle passing data between tasks
    - Adding a schedule to automate report generation
    - Adding `inserted_date` and `updated_date` as basic metadata on report creation

***
