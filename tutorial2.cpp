#include <cstdlib>
#include "mongo/client/dbclient.h" // for the driver
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>


using namespace std;
using namespace mongo;
namespace fs = boost::filesystem; 

int main(int argc, char *argv[])
{
    client::initialize();
  DBClientConnection c;
  c.connect("localhost");
  
  string file(argv[1]);
  ifstream csvFile;
  string dropheader, line;
  csvFile.open(file);
  getline(csvFile, dropheader);
  while(getline(csvFile, line, ';')){
    /*
      BSONObj doc = BSON(GENOID, "Date" << line << "Open" << line
      << "High" << line << "Low" << line << "Close"
      << line << "Vol" << line);
      cout << doc.toString() << endl;
    */
    cout << line << endl;
  }
  
  return EXIT_SUCCESS;
}


