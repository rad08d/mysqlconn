__author__ = 'alandinneen'

from MySQLdb import connect, Error
from collections import OrderedDict

class DBConn(object):
    """
    A class to handle all MySQL connection reads/writes.
    """

    def __init__(self, host=None, db=None, user=None, password=None):
        self._host = host
        self._db = db
        self._user = user
        self._pass = password


    def connect(self):
        """
        Return a connect object to MySQL database
        """
        db = connect(self._host, self._user, self._pass, self._db)
        return db

    def close(self, conobj):
        """
        Close connection to MySQL database
        """
        conobj.close()
        return None

    def select(self, sqlselect):
        """
        Simple method for performing MySQL select.
        """
        try:
            db = self.connect()
            cursor = db.cursor()
            cursor.execute(sqlselect)
            rawdata = cursor.fetchall()
            cursor.close()
            db.close()
            return rawdata
        except Error, e:
            print "There has been an error in the select! " + e

    def single_insert(self, sqlinsert):
        """
        Performs a single insert and returns the last inserted id for the session.
        """
        try:
            insertid = None
            db = self.connect()
            cursor = db.cursor()
            cursor.execute(sqlinsert)
            cursor.execute("SELECT LAST_INSERT_ID();")
            rowid = cursor.fetchall()
            insertid = rowid[0][0]
            cursor.close()
            db.commit()
            return insertid
        except Exception as e:
            db.rollback()
            print "There has been an error in the single insert. The transaction has been rolled back. Error: " + e
        finally:
            db.close()

    def mass_insert(self, sqlstatment, dbobj):
        """
        Provides a mass insert mechanism. This method needs to be provided a self.connect() object before calling
        this method. It should always be followed with a call to self.close()
        """
        try:
            cursor = dbobj.cursor()
            cursor.execute(sqlstatment)
        except Exception as e:
            dbobj.rollback()
            print "There has been an error in the single insert. The transaction has been rolled back. Error: " + e

    def update(self, sqlupdate):
        """
        Opens a MySQL connection, performs an update, then closes the connection. Rolls back any changes if an
        error occurs.
        """
        try:
            insertid = None
            db = self.connect()
            cursor = db.cursor()
            cursor.execute(sqlupdate)
            cursor.close()
            db.commit()
        except Exception as e:
            db.rollback()
            print "There has been an error in the single insert. The transaction has been rolled back. Error: " + e
        finally:
            db.close()

    def cursor_results_to_dict(self, results):
        """
        Returns a list of OrderedDicts from the cursor results
        """
        data = []
        if results.rowcount:
            keys = results.keys()
            for row in results:
                obj = OrderedDict()
                for key in keys:
                    obj[key] = str(row[key]).decode('UTF-8', 'ignore')
                data.append(obj)
        return data