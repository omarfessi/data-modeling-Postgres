#import the needed libraries:
import psycopg2 # python driver to set connexion to Postgres
def create_database():
	#connect to the default database :
	conn=psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=admin" )
	conn.set_session(autocommit=True)
	cur=conn.cursor()

	#drop sparkifydb if it exists 
	cur.execute("DROP DATABASE IF EXISTS sparkifydb")
	#create sparkifydb where to host all tables 
	cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

	#close the connection to the default database
	conn.close()



def main():
	create_database()

if __name__=="__main__":
	main()



