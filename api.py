from flask import Flask,jsonify,make_response,request
import mysql.connector
import logging
import jwt
# from werkzeug.security import generate_password_hash,check_password_hash
import datetime
from functools import wraps

app = Flask(__name__)
app.config['SECRET_KEY'] = 'THISISASECTREETASASDFS'



def token_required(f):
  @wraps(f)
  def decorated(*args, **kwargs):
    token = None
    if 'token' in request.headers:
      token = request.headers['token']
    if not token:
      return jsonify({'message': 'Token is missing'})

    try:
      data = jwt.decode(token,app.config['SECRET_KEY'],algorithms=['HS256',])

      current_user = data['user']
    except Exception as e:
      return jsonify({'message':'Token is invalid'})

    return f(current_user,*args,**kwargs)
  return decorated




@app.route('/')
def home():
  return "WELCOME"

@app.route('/pipelines/<int:account_id>')
@token_required
def configured_pipeline_details(current_user,account_id):
  mydb = mysql.connector.connect(
    host="decision-tree.cxhedmomc3jg.us-east-1.rds.amazonaws.com",
    user="dc_read_only",
    password="A4abzG2Ar2Bycc8A",
    database="dc_admin_staging"
  )
  li = {}
  try:
    mycursor = mydb.cursor()
    sql = f"select name from accounts where id={account_id}"
    mycursor.execute(sql)
    li.update({'Account name':mycursor.fetchall()})
    sql = f'SELECT account_id,datasource_id,count(cr.id),cr.status,d.name from dc_admin_staging.configured_reports cr join datasources d on cr.datasource_id =d.id WHERE account_id = {account_id} GROUP BY 1,2,4'
    mycursor.execute(sql)
    for i in mycursor.fetchall():
      if f'Datasource = {i[4]}' in li:
        li[f'Datasource = {i[4]}'].append((i[2],i[3]))
      else:
        li[f'Datasource = {i[4]}'] =['number of pipelines',(i[2],i[3])]
  except Exception as e:
    logging.debug(str(e))
  finally:
    mycursor.close()
  return jsonify(li)

@app.route('/warehouse/<int:account_id>')
def warehouse_connection_host(account_id):
  mydb = mysql.connector.connect(
    host="decision-tree.cxhedmomc3jg.us-east-1.rds.amazonaws.com",
    user="dc_read_only",
    password="A4abzG2Ar2Bycc8A",
    database="dc_admin_staging"
  )
  li = {}
  try:
    mycursor = mydb.cursor()
    sql = f"select name from accounts where id={account_id}"
    mycursor.execute(sql)

    li.update({'Account name':mycursor.fetchall()})
    sql = f"select db_conn_host,dd.name from data_warehouses join data_destinations dd on data_warehouses.data_destination_id = dd.id where account_id = {account_id}"
    mycursor.execute(sql)
    li.update({'Warehouses':[f'host:{i[0]} and type : {i[1]}' for i in mycursor.fetchall()]})
  except Exception as e:
    logging.debug(str(e))
  finally:
    mycursor.close()
  
  return jsonify(li)


@app.route('/login')
def login():
  auth = request.authorization
  if not auth or not auth.password or not auth.username:
    return make_response('could not verify',401,{'www-Authenticate':'Basic realm="Login Required"'})
  
  token  = jwt.encode({'user':auth.username, 'exp':datetime.datetime.utcnow()+datetime.timedelta(minutes=1)},app.config['SECRET_KEY'])
  if token:
    return jsonify({'token':token})
  return make_response('could not verify',401,{'www-Authenticate':'Basic realm="Login Required"'})

if __name__ == '__main__':
    app.run(debug=True)






# SELECT
# 	account_id,
# 	datasource_id,
# 	count(id),
# 	status
# from
# 	dc_admin_staging.configured_reports cr
# WHERE
# 	account_id = 2
# 	and datasource_id = 3
# GROUP BY
# 	1,
# 	2,
# 	4




