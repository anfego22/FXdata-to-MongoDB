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
using namespace eigen;

class FXtoBSON{
 public:
  MatrixXd Hour;
  MatrixXd Day;
  MatrixXd Month;
  ifstream csvFile;
  string file, formatt, db, dbH, dbD, dbM;
  char sep;
  struct tm time0;
  int cols, rows;
  vector<string> names;
  FXtoBSON(const string &file_, const string &formatt_,
	   const string &pair, const string &source,
	   const char &sep);
  void headers();
  void getTime0();
  void updateDay(const struct tm &, DBClientConnection &);
  void toEigen(const int &, const BSONObj &);
  void aggregate(const int &);
  BSONObj headerQuote(const string &line, struct tm & temp);
  BSONObj buildQuoteAt(const int & min, const BSONObj & QUOTE);
  BSONObj emptyHour();
  BSONObj find(struct tm tempTM, const int &a);
  BSONObj dayDoc();
};

#endif
