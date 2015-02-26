#include "FXtoBSON.h"
#include <cstdlib>


using namespace std;
using namespace mongo;

int main(int argc, char *argv[])
{
  string file, formatt, pair;
  char sep;
  if(argc < 5 & argc != 1){
    cout << "Specify file, formatt, pair and separator" << endl;
    return 0;
  }
  if(argc == 1){
    file = "/home/anfego/Dropbox/OneDrive-2014-08-25/Documentos/FOREX/Data/data2cut.csv";
    formatt = "%Y%m%d%H%M%S";
    pair = "eurusd";
    sep = ';';
  }
  if(argc == 5){
    file = argv[1];
    formatt = argv[2];
    pair = argv[3];
    sep = *argv[4];
  }
  mongo::client::initialize();
  DBClientConnection c;
  try {
    c.connect("localhost");
  } catch( const mongo::DBException &e ) {
    std::cout << "caught " << e.what() << std::endl;
  }
  FXtoBSON fxbson(file, formatt, pair, c, sep);
  return EXIT_SUCCESS;
}


