import time, sys, cherrypy, os
# Path for spark source folder
os.environ['SPARK_HOME']="/spark-2.0.1-bin-hadoop2.7"

# Append pyspark  to Python Path
sys.path.append("/spark-2.0.1-bin-hadoop2.7/bin")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)


from paste.translogger import TransLogger
from app import create_app


def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("movie_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
 
    return sc
 
 
def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 5432,
        'server.socket_host': '0.0.0.0'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
 
 
if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    #print(os.path)
    #dataset_path = os.path.join('datasets', 'ml-latest')
    dataset_path = "file:////home/aashishkatlam/spark-movie-lens/Datasets/ml-latest-small"
    app = create_app(sc, dataset_path)
 
    # start web server
    run_server(app)

