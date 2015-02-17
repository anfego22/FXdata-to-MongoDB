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

void FXtoBSON(const string &file){
  ifstream csvFile;
  string dropheader, line, cell;
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  while(getline(csvFile, line)){
    string tmTime;
    stringstream lineS(line);
    vector<double> quotes;
    quotes.reserve(5);
    for (int i = 0; i<=5; i++){
      getline(lineS, cell, ';');
      istringstream ss(cell);
      switch(i){
      case 0:
	{
	  ss >> tmTime;
	}
	break;
      default:
	{
	  double d;
	  ss >> d;
	  quotes.push_back(d);
	}
	break;
      }
    }
    BSONObjBuilder dc;
    dc.append("Date", tmTime).append("Open", quotes[0]);
    dc.append("High", quotes[1]).append("Low", quotes[2]);
    dc.append("Close", quotes[3]).append("Vol", quotes[4]);
    BSONObj doc = dc.obj();
    cout << doc.toString() << endl;
  }
}

#endif
