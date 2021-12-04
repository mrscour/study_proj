import os
import json
from flask import Flask, render_template, url_for
from google.cloud import bigquery

bigquery_client = bigquery.Client()
app = Flask(__name__)

@app.route("/")
def hello_world():
    return f'''<h1>
Total users records:<br>
<a href="{ url_for('full_list') }">Total_users</a><br>
Total transactions records:<br>
<a href="{ url_for('transactions_full') }">Total_transactions</a><br>
Short statistic for revenue:<br>
<a href="{ url_for('short_revenue') }">Short_revenue</a>
</h1>'''

@app.route('/total-users', methods=['GET'])
def full_list():
    # Define query_job and query object
    query_job = bigquery_client.query(
        """
        SELECT
            users_test.uid,
            users_test.user_name,
            users_test.user_phone,
            users_test.user_email,
            users_test.currency,
            MAX(users_test.debit_amount) as debit_amount,
            MAX(users_test.credit_amount) as credit_amount
        FROM study_project.users_test
        GROUP BY users_test.uid, users_test.currency,
        users_test.user_name, users_test.user_email, users_test.user_phone
        ORDER BY users_test.uid;
        """
    )
    # Handle query_job result and return to flask to display
    res = query_job.result()
    records = [dict(row) for row in res]
    json_res = json.dumps(str(records))
    return json_res
    
@app.route('/transactions-total', methods=['GET'])
def transactions_full():
    # Define query_job and query object
    # Transactions made per user
    query_job = bigquery_client.query(
        """
        SELECT * FROM study_project.trans_test;
        """
    )

    # Handle query_job result and return to flask to display
    res = query_job.result()
    records = [dict(row) for row in res]
    json_res = json.dumps(str(records))
    return json_res

@app.route('/short-revenue', methods=['GET'])
def short_revenue():
    # Define query_job and query object
    # Transactions made per user
    query_job = bigquery_client.query(
        """
        SELECT
        ROUND(SUM(t.win_amount/t.conversion_rate_EUR), 2) total_win_amount_eur,
        ROUND(SUM(t.bet_amount/t.conversion_rate_EUR), 2) total_bet_amount_eur,
        ROUND((SUM(t.bet_amount/conversion_rate_EUR) - SUM(t.win_amount/t.conversion_rate_EUR)), 2) casino_total_profit_eur,
        MIN(t.timestamp) start_date,
        MAX(t.timestamp) end_date
        FROM
        study_project.trans_test t;
        """
    )
    # Handle query_job result and return to flask to display
    res = query_job.result()
    records = [dict(row) for row in res]
    json_res = json.dumps(str(records))
    return json_res

if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8080')
    app.run(debug=False, port=server_port, host='0.0.0.0')