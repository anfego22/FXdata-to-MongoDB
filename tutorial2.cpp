#include "FXtoBSON.h"
#include <cstdlib>


using namespace std;
using namespace mongo;

int main(int argc, char *argv[]){
  string file, formatt, pair, source;
  char sep;
  if(argc < 6 & argc != 1){
    cout << "Specify file" << endl;
    cin >> file;
    cout << "formatt" << endl;
    cin >> formatt;
    cout << "pair" << endl;
    cin >> pair;
    cout << "source" << endl;
    cin >> source;
  }
  if(argc == 1){
    file = "/home/anfego/Dropbox/OneDrive-2014-08-25/Documentos/FOREX/Data/data2cut.csv";
    formatt = "%Y%m%d%H%M%S";
    pair = "eurusd";
    sep = ';';
    source = "fxHistorical";
  }
  if(argc == 6){
    file = argv[1];
    formatt = argv[2];
    pair = argv[3];
    sep = *argv[4];
    source = argv[5];
  }
  FXtoBSON fxbson(file, formatt, pair, source, sep);
  return EXIT_SUCCESS;
}


