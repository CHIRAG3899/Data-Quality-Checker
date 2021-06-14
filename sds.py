import datetime
import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
import pandas as pd
import gcsfs
import json
from google.cloud import storage
from google.oauth2 import service_account
import csv
context = ge.data_context.DataContext();
dg = ge.read_csv('gs://hr_data_project/Employee_monthly_salary.csv',encoding= 'unicode_escape');
expectation_suite_name = "Employee_Table"; # this is just an example
context.create_expectation_suite(expectation_suite_name, overwrite_existing=True);
batch_kwargs = {
    'dataset': dg,
    'datasource': 'pandas'
};
batch = context.get_batch(batch_kwargs, expectation_suite_name);
batch.expect_column_to_exist('EmpID','Name','Gender','Age','Designation','Department','GROSS','Deduction_percentage',result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_values_to_be_unique('EmpID',result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_values_to_not_be_null('EmpID',result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_values_to_be_of_type('Designation','str',result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_values_to_be_of_type('EmpID','int64',result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_values_to_be_in_type_list('Designation',['int','int64','str'],result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_distinct_values_to_be_in_set('Gender',['M','F'],result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_value_lengths_to_be_between('Deduction_percentage', min_value=0, max_value=100,result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_mean_to_be_between('Age',min_value=20,max_value=35,result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_median_to_be_between('Age',min_value=20,max_value=35,result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_median_to_be_between('Deduction_percentage',result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_mean_to_be_between('Deduction_percentage',result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_stdev_to_be_between('Deduction_percentage',min_value=-2, max_value=2,result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_stdev_to_be_between('GROSS',min_value=-2, max_value=2,result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_pair_values_A_to_be_greater_than_B('GROSS','Net_Pay',ignore_row_if='either_value_is_missing',result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_min_to_be_between('Age',min_value=20, max_value=22,result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_pair_values_A_to_be_greater_than_B('GROSS','Net_Pay',ignore_row_if='either_value_is_missing',result_format='COMPLETE',catch_exceptions=None);
batch.expect_select_column_values_to_be_unique_within_record(['GROSS','Net_Pay','Deduction'], ignore_row_if='any_value_is_missing', result_format='COMPLETE',catch_exceptions=None);
batch.expect_compound_columns_to_be_unique(['GROSS','Net_Pay','Deduction'], ignore_row_if='any_value_is_missing', result_format='COMPLETE',catch_exceptions=None);
batch.expect_column_values_to_match_strftime_format('Date_of_Birth',strftime_format='DD/MM/YY',row_condition='Date_of_Birth>="01/01/1992" & Date_of_Birth>="01/01/92" & Date_of_Birth>="1/1/92"',condition_parser='pandas',result_format='COMPLETE',catch_exceptions=None);
batch.save_expectation_suite(discard_failed_expectations=False);

results = LegacyCheckpoint(
    name="_temp_checkpoint",
    data_context=context,
    batches=[
        {
          "batch_kwargs": batch_kwargs,
          "expectation_suite_names": [expectation_suite_name]
        }
    ]
).run();
validation_result_identifier = results.list_validation_result_identifiers()[0];
p=results.success;
print(p)
if p==True:
        dg.to_gbq(destination_table = 'cgcg.cgcg', project_id='mythic-ego-316314', location='EU',if_exists='replace');
if p==False:
        context.build_data_docs();
        context.open_data_docs(validation_result_identifier);
