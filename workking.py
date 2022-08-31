import great_expectations as ge
context = ge.data_context.DataContext()
suite = context.create_expectation_suite(
    'check_avocado_data',
    overwrite_existing=True
)
batch_kwargs = {
    'path': 'data/avocado.csv',
    'datasource': 'data_dir',
    'data_asset_name': 'avocado',
    'reader_method': 'read_csv',
    'reader_options': {
        'index_col': 0,
    }
}
batch = context.get_batch(batch_kwargs, suite)
batch.head()
batch.expect_column_to_exist('Date')
batch.expect_column_values_to_not_be_null('region')
batch.expect_column_values_to_be_of_type('region', 'str')
batch.expect_column_values_to_be_of_type('region', 'int')
batch.expect_column_values_to_match_strftime_format('Date', "%Y-%m-%d")
batch.expect_column_values_to_match_strftime_format('Date', "%m-%Y-%d")
batch.expect_column_values_to_be_between('AveragePrice', min_value=0.5, max_value=3.0)
partition_object = {
    'values': ['conventional', 'organic'],
    'weights': [0.5, 0.5],
    
}
batch.expect_column_kl_divergence_to_be_less_than('type', partition_object, 0.1)
batch.get_expectation_suite()
results = context.run_validation_operator('my_validation_operator', assets_to_validate=[batch])
results
batch.save_expectation_suite()



partition_object = {
    'values': ['conventional', 'organic'],
    'weights': [0.5, 0.5],
    
}
batch.expect_column_to_exist('Date').expect_column_values_to_not_be_null('region').expect_column_values_to_be_of_type('region', 'str').expect_column_values_to_be_of_type('region', 'int').expect_column_values_to_match_strftime_format('Date', "%Y-%m-%d").expect_column_values_to_match_strftime_format('Date', "%m-%Y-%d").expect_column_values_to_be_between('AveragePrice', min_value=0.5, max_value=3.0).expect_column_kl_divergence_to_be_less_than('type', partition_object, 0.1)
batch.get_expectation_suite()
results = context.run_validation_operator('my_validation_operator', assets_to_validate=[batch])
results
batch.save_expectation_suite()

with open(jsonschema_file, "r") as f:
    raw_json = f.read()
    schema = json.loads(raw_json)