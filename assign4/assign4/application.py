#################################################################################
##  Name: Suyog S Swami
##  ID: 1001119101
##  Course: CSE6331(Cloud Computing) Batch : 1:00PM to 3:00PM
##  Assignment 5: AWS(WEB)
##  References:     http://docs.aws.amazon.com/elasticbeanstalk/latest/dg/create-deploy-python-flask.html
##                  http://code.runnable.com/UhLMQLffO1YSAADK/handle-a-post-request-in-flask-for-python
##                  http://boto.readthedocs.org/en/latest/dynamodb2_tut.html
##                  http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html
#################################################################################
from flask import Flask,render_template,request,url_for
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey,RangeKey,KeysOnlyIndex,GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
import time

# This section needs to be executed separately as the numbers of records in CSV takes a lot of time to get inserted in DynamoDB

# Create connection to DynamoDB
conn = boto.dynamodb2.connect_to_region(
        )

#Create Table in DynamoDB
consumer_complaints=Table.create('Customer_complaints',
                schema=[HashKey('Complaint_ID'),RangeKey('Product')],
                throughput={'read':15,'write':15},connection=conn)

# Insert records from csv to DynamoDB table
with open('Consumer_Complaintss.csv', 'rb') as csvfile:
    spamreader = csv.reader(csvfile)
    #print spamreader
    next(spamreader)
    for row in spamreader:
        consumer_complaints.put_item(data={'Complaint_ID':str(row[0]),
                                    'Product':str(row[1]),
                                    'Sub-product':str(row[2]),
                                    'Issue':row[3],
                                    'Sub-issue':row[4],
                                    'State':row[5],
                               	    'ZIP code':row[6],
                                    'Submitted via':row[7],
                                    'Date received':row[8],
                                    'Date_sent_to_company':row[9],
                                    'Company':row[10],
                                    'Company_response':row[11],
                                    'Timely response':row[12],
                                    'Consumer_disputed':str(row[13])})

# Get record after inserting (to check that data is inserted)
cc=consumer_complaints.get_item(Complaint_ID='1289961',Product='Credit reporting')
print cc['Issue'],cc['State']

# Describe table details
c=conn.describe_table('Customer_complaints')
print c

#################################################################################
# This section implements Flask framework as UI for displaying the results
# You can comment the upper part after inserting records (Keep the libraries as it is.)

# Table variable
consumer_complaints=Table('Customer_complaints',connection=conn)

app = Flask(__name__)

@app.route('/')
def form():
    return render_template('form_submit.html')

# For executing equal section of queries:
@app.route('/equal/', methods=['POST'])
def equal():
    select=request.form.getlist('select')
    select_en=[]
    for s in select:
        s=str(s)
        select_en.append(s)
    state=request.form.get('state')
    product=request.form.get('product')
    cc1=consumer_complaints.scan(State__eq=state,Product__eq=product)
    return render_template('form_action.html', select_en=select_en,cc1=cc1)


# For executing the contains and beginswith
@app.route('/contains/', methods=['POST'])
def contains():
    select=request.form.getlist('select')
    select_en=[]
    for s in select:
        s=str(s)
        select_en.append(s)
    issue=request.form.get('issue')
    company=request.form.get('company')
    time1=time.time()
    cc1=consumer_complaints.scan(Issue__contains=issue,Company__beginswith=company)
    time2=time.time()
    time3=time2-time1
    return render_template('form_action.html',time3=time3, select_en=select_en,cc1=cc1)

if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    app.debug = True
    app.run()
