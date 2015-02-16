#include <cstdlib>
#include "mongo/client/dbclient.h" // for the driver
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <vector>

using namespace std;
using namespace mongo;

int main(int argc, char *argv[])
{
  mongo::client::initialize();
  DBClientConnection cc;
  cc.connect("localhost");
  
  std::string file(argv[1]);
  std::ifstream csvFile;
  std::string dropheader, line, cell;
  csvFile.open(file);
  getline(csvFile, dropheader);
  while(getline(csvFile, line)){
    std::string tmTime;
    std::stringstream lineS(line);
    vector<double> quotes;
    quotes.reserve(5);
    for (int i = 0; i<=5; i++){
      getline(lineS, cell, ';');
      std::istringstream ss(cell);
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
  return EXIT_SUCCESS;
}


