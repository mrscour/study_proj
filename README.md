# very first study project
Example start launch string for data generator:

cd ~/study_proj
python data_gen.py \
--topic topic1 \
--project-id tribal-bonsai-330115 \
--enable-log true \
--sleep_time 100

python process_dataflow.py \
--topic topic1 \
--project_id tribal-bonsai-330115 \
--input_subscription projects/tribal-bonsai-330115/subscriptions/mySub1 \
--bigquery_dataset study_project \
--bigquery_table trans_test users_test
