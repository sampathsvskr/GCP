from airflow.utils.email import send_email_smtp

def report_failure(context):
    task = context['task'].task_id
    dagid = context['task_instance'].dag_id
    lohurl = context['task_instance'].log_url
    exception = context.get('exception')

    subject = "job failure"
    html_content = "job failure"
    to_emails = ["sampath@gmail.com"]
    send_email_smtp(to_emails, subject, html_content)



def report_failure_1(context):
    
    send_email = EmailOperator(task_id = "failure_email", to = ["sampath@gmail.com"], subject = "job failure", html_content = "job failure", dag = dag)
    send_email.execute(context)