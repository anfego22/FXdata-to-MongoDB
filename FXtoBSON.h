#ifndef FX_TO_BSON
#define FX_TO_BSON
#include "mongo/client/dbclient.h" // for the driver
#include "mongo/bson/bson.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>
#include <time.h>
#include "Eigen/Dense"

using namespace std;
using namespace mongo;
using namespace Eigen;

class FXtoBSON{
 public:
  MatrixXd Hour, Day, Month, Year;
  ifstream csvFile;
  string file, formatt, db, dbH, dbD, dbM, dbY;
  char sep;
  struct tm time0;
  int cols, rows;
  BSONObj projId;
  vector<string> names;
  FXtoBSON(const string &, const string &,
	   const string &, const string &,
	   const char &);
  void headers();
  void getTime0();
  void updateDoc(const char &, const struct tm &,
		 DBClientConnection &);
  void hourToEigen(const int &, const BSONObj &);
  void addMinToDB(const struct tm &,
		   const BSONObj & ,
		   DBClientConnection &);
  void aggregateToDB(const char &,
		     const struct tm &,
		     DBClientConnection &);
  VectorXd reduce(const char &);
  BSONObj emptyDoc(const char &);
  BSONObj aggregate(const char &, const struct tm &);
  BSONObj headerQuote(const string &line, struct tm & temp);
  BSONObj buildQuoteAt(const int & min, const BSONObj & QUOTE);
  BSONObj find(struct tm tempTM, const char &a);
};

#endif
