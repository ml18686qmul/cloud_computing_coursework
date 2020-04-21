
m flask import jsonify
import requests
from cassandra.cluster import Cluster
import requests_cache
from flask import Response
from flask import Flask, render_template, request, jsonify
import json
from passlib.hash import sha256_crypt

requests_cache.install_cache('carbon_api_cache', backend='sqlite', expire_after=36000)
cluster = Cluster(['cassandra'])
KEYSPACE = "cloud"
session = cluster.connect()
cloud = Flask(__name__, instance_relative_config = True)
cloud.config.from_object('config')
cloud.config.from_pyfile('config.py')
MY_API_KEY = cloud.config["MY_API_KEY"]
carbon_url_template = "https://api.carbonintensity.org.uk/intensity/date/2017-10-01?forcast={forcast}&actual={actual}&key={API_KEY}&start_datetime={from}&end_datetime={to}"


# Hash-based authentication: user accounts and access management.
def authorization(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if auth and auth.username == 'admin' and auth.password == 'pass':
            return f(*args, **kwargs)

        return make_response('Login not Successfu', 401, {'WWW-Authenticate': 'Basic realm= "Login needed"'})

    return decorated

@app.route('/', methods=['GET'])
def Hello():
   if (request.authorization and sha256_crypt.verify(request.authorization.username,'admin')
           and sha256_crypt.verify(request.authorization.password,'pass')):

        my_forcast = request.args.get('forcast','248')
        my_actual = request.args.get('actual','209')
        my_start = request.args.get('from','2017-10-01T23:30Z')
        my_end = request.args.get('to','2017-10-02T00:00Z')
        carbon_url = carbon_url_template.format(lat=my_forcast, lng=my_actual,API_KEY=MY_API_KEY, start=my_start, end=my_end)
        resp = requests.get(carbon_url)

        start = []
        index = []

        x = range(100)
        for i in x:
            start.append(resp.json()['data'][i]['from'])
            index.append(resp.json()['data'][i]['index'])

        for i in x:
            session.execute(
                """INSERT INTO myFile(start, index)VALUES (%(start)s, %(index)s)""",
                {'start': start[i], 'index': str(index[i])})

            rows = session.execute('SELECT * FROM myFile')

            output = ""
            for row in rows:
                output = output + "start:" + str(row.start) + "index:" + str(
                    row.index)  + "<br />"
        #    session.execute("DROP KEYSPACE " + KEYSPACE)
        if output == "":
            return ("The database is empyty")
        else:
            return json.dumps(output)
   return Response('Could not verify', 401, {'WWW-Authenticate': 'Basic realm="Login Required"'})

   return Response('Could not verify',401,{'WWW-Authenticate': 'Basic realm="Login Required"'})


# ------1-GET
@app.route('/', methods=['GET'])
def Records_GET(day,time):
    output=""
    rows = session.execute("""SELECT * FROM myFile""")
    time = "2017-10-"+day+"T"+time+":00:00Z"
    for row in rows:
        if (row.start==time):
            output = output+"start:"+str(row.start)+"index:"+str(row.index)+"<br />"

    if output=="":
        return("Fail")
    else:
        return json.dumps(output)

# ------2-PUT
@app.route('/', methods=['GET','PUT'])
def Records_PUT(day,time,pollutant):
    rows = session.execute("""SELECT * FROM myFile""")
    time = "2017-10-"+day+"T"+time+":00:00Z"
    for row in rows:
        if (row.start == time):
            index = row.index
            session.execute("""DELETE From myFile WHERE datetime = '{}'""".format(time))
            session.execute("""INSERT INTO myFile(start, index)VALUES (%(start)s, %(index)s)""",{'start': time, 'index': index})
            return "Succesful!"
        else:
            return  "Oops, fail!"

# ------3-POST
@app.route('/', methods=['GET','POST'])
def Records_POST(day,start,index):
    output=""
    rows = session.execute("""SELECT * FROM myFile""")
    for row in rows:
        time = "2017-10-"+day+"T"+time+":00:00Z"

        if ((row.start)!=time):
            session.execute("""INSERT INTO myFile(start, index)VALUES (%(start)s, %(index)s)""",{'start': time, 'index': index})
            output = output+"start:"+time+"index:"+index+"<br />"
            return json.dumps("Successful!    "+"<br />"+output)
    return ("Fail, beacause it already exists!")

# -----4-DELETE
@app.route('/', methods=['GET','DELETE'])
def Records_DELETE(day,time):
    time = "2017-10-"+day+"T"+time+":00:00Z"
    session.execute( """DELETE From myFile WHERE start = '{}'""".format(time))
    rows = session.execute("""SELECT * FROM myFile""")
    for row in rows:
        if (row.start==time):
            return "Fail to delete"
    return "Succesful!"


if __name__=="__main__":
        cloud.run(host='0.0.0.0', port= 80, debug=True)

