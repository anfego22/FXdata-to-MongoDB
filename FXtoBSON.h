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
  string file, formatt;
  int T, cols, h;
  BSONObjBuilder dc, bArr;
  BSONObj bab;
  vector<BSONObj> docs;
  vector<string> names;
  FXtoBSON(const string &file_, const string &formatt_,
	   DBClientConnection &c);
  void rowFile();
  void printBSON();
  void headers();
};

#endif
