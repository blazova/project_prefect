from prefect import task, Flow

@task
def extract_reference_data():
    # fetch reference data
    ...
    return reference_data

@task
def extract_live_data():
    # fetch live data
    ...
    return live_data

@task
def transform(live_data, reference_data):
    # clean the live data
    ...
    return transformed_data

@task
def load_reference_data(reference_data):
    # save reference data to the database
    ...

@task
def load_live_data(transformed_data):
    # save transformed live data to the database
    ...


with Flow("Aircraft-ETL") as flow:
    reference_data = extract_reference_data()
    live_data = extract_live_data()

    transformed_live_data = transform(live_data, reference_data)

    load_reference_data(reference_data)
    load_live_data(transformed_live_data)

flow.run()
