# whalesafe-api

## r plumber api

getting empty rows!? see [whalesafe:simplify_segments.Rmd](https://github.com/BenioffOceanInitiative/whalesafe/blob/91a06380bece08363652ee4813a8c5cae061c084/simplify_segments.Rmd#L86-L92)

## python flask api

### references

* [Installation — Flask Documentation (1.1.x)](https://flask.palletsprojects.com/en/1.1.x/installation/)
  * [How to install python3-venv on Ubuntu 16.04 - Techcoil Blog](https://www.techcoil.com/blog/how-to-install-python3-venv-on-ubuntu-16-04/)
* [Quickstart — Flask Documentation (1.1.x)](https://flask.palletsprojects.com/en/1.1.x/quickstart/)
* [Build and deploy a Flask CRUD API with Cloud Firestore and Cloud Run](https://cloud.google.com/community/tutorials/building-flask-api-with-cloud-firestore-and-deploying-to-cloud-run)
* [Mechanicalgirl.com](http://www.mechanicalgirl.com/post/using-google-cloud-functions-create-simple-post-endpoint-handle-data/)
* [geojson · PyPI](https://pypi.org/project/geojson/)


### install flask

* [Installation — Flask Documentation (1.1.x)](https://flask.palletsprojects.com/en/1.1.x/installation/)
  * [How to install python3-venv on Ubuntu 16.04 - Techcoil Blog](https://www.techcoil.com/blog/how-to-install-python3-venv-on-ubuntu-16-04/)

Via [rstudio.ships4whales.org](http://rstudio.ships4whales.org) Terminal:

* [Quick start — Flask-RESTX 0.2.1.dev documentation](https://flask-restx.readthedocs.io/en/latest/quickstart.html)
* [Full example — Flask-RESTX 0.2.1.dev documentation](https://flask-restx.readthedocs.io/en/latest/example.html)

```bash
# install py3 env
sudo apt-get update
sudo apt-get install python3-venv -y

# create environment
mkdir py3env
cd ~/py3env
python3 -m venv venv

# activate the environment
cd ~/py3env
. venv/bin/activate

# install flask
pip install Flask
```

### hello.py

* [Quickstart — Flask Documentation (1.1.x)](https://flask.palletsprojects.com/en/1.1.x/quickstart/)

write to `hello.py` in /home/admin/plumber-api:

```python
from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'
```

Redo install packages for whalesafe `api.py`:

```bash
pip install Flask
pip install flask-restx
#pip install flask-restplus
pip install werkzeug
pip install pandas
pip install geojson
pip install google-cloud-bigquery
pip install google-oauth
```


Run:

```bash
# activate the environment (if not already)
cd ~/py3env
. venv/bin/activate

# set env vars
#export FLASK_APP=/home/admin/plumber-api/hello.py
export FLASK_APP=/home/admin/plumber-api/flask-restplus-examples/complex.py
export FLASK_APP=/home/admin/plumber-api/flask-restplus-examples/todo_simple.py
export FLASK_APP=/home/admin/plumber-api/api.py
flask run --host=0.0.0.0 --port=8888 &

# run locally
flask run

# kill r plumber
ps -eaf | grep ws-api # get process id (pid), eg 99999
sudo kill -9 99999

# run so externally visible
flask run --host=0.0.0.0 --port=8888 &

# kill flask
ps -eaf | grep flask # get process id (pid), eg 99999
sudo kill -9 99999

# start r plumber
sudo Rscript /srv/ws-api/run_api.R &

# leave python virtual environment
deactivate
```

Next Steps: 

* [Deployment Options — Flask Documentation (1.1.x)](https://flask.palletsprojects.com/en/1.1.x/deploying/#deployment)
  * [Python 3 Runtime Environment](https://cloud.google.com/appengine/docs/standard/python3/runtime)