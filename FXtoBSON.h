#ifndef FX_TO_BSON
#define FX_TO_BSON
#include <cstdlib>
#include "mongo/client/dbclient.h" // for the driver
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>

using namespace std;
using namespace mongo;

class FXtoBSON{
 public:
  ifstream csvFile;
  string file;
  int T, cols;
  vector<BSONObj> docs;
  vector<string> names;
  FXtoBSON(const string &file_);
  void rowFile();
  void printBSON();
  void headers();
};

#endif
