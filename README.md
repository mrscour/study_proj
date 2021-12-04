# DataART study project
Example start launch string for data generator:

cd ~/study_proj
python data_gen.py \
--topic topic1 \
--project-id tribal-bonsai-330115 \
--enable-log true \
--sleep_time 100 \
--api-key a21749a**********9b0fb04
# api key used for access to exchange api

Example launch string for data processor on Direct runner:

python process_dataflow.py \
--topic topic1 \
--project_id tribal-bonsai-330115 \
--input_subscription projects/tribal-bonsai-330115/subscriptions/mySub1 \
--bigquery_dataset study_project \
--bigquery_table trans_test users_test

Example launch string for data processor on Custom Job runner:

python process_dataflow.py \
--topic topic1 \
--project_id tribal-bonsai-330115 \
--input_subscription projects/tribal-bonsai-330115/subscriptions/mySub1 \
--bigquery_dataset study_project \
--bigquery_table trans_test users_test \
--runner DataflowRunner \
--job_name dataflow-custom-pipeline-v2 \
--region us-central1 \
--temp_location gs://custom_storage_v1/ \
--project tribal-bonsai-330115 \
--max_num_workers 2
