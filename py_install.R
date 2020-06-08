library(reticulate)
py_install("pandas")
py_install("google-cloud-bigquery")
py_install("sqlalchemy")
py_install("psycopg2")
py_install("tzlocal")
#py_install("google-oauth")
#py_install("geopandas")
#py_install("Shapely")
#py_install("numpy")
conda_list()

# sudo apt install python3-pip
# pip3 --version
# pip3 install pandas sqlalchemy psycopg2 google-cloud-bigquery tzlocal
# which python3
# /usr/bin/python3