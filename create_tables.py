#import the needed libraries:
import psycopg2 # python driver to set connexion to Postgres
def create_database():
	#connect to the default database :
	conn=psycopg2.connect(host="127.0.0.1 dbname=postgres user=postgres password=admin" )
	conn.set_session(autocommit=True)
	cur=conn.cursor()