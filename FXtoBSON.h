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

using namespace std;
using namespace mongo;

class FXtoBSON{
 public:
  ifstream csvFile;
  string file, formatt, db;
  int T, cols, h;
  BSONObjBuilder MIN, YEAR, HOUR, DAY, MONTH;
  vector<string> names;
  FXtoBSON(const string &file_, const string &formatt_,
	   const string &pair, DBClientConnection &c);
  void headers();
  void Years();
};

#endif
