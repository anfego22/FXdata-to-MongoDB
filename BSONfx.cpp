#include "FXtoBSON.h"

using namespace std;
using namespace mongo;

void FXtoBSON::rowFile(){
  if(T == 0){
  csvFile.seekg(0);
  csvFile.open(file.c_str());
  string line;
  while(getline(csvFile, line)){
    T++;
  }
  T -= 1;
  csvFile.close();
  }
}

void FXtoBSON::headers(){
  if(names.size() == 0){
    csvFile.seekg(0);
    csvFile.open(file.c_str());
    string cell, firstline;
    getline(csvFile, firstline);
    stringstream lineS(firstline);
    while(getline(lineS, cell, ';')){
      istringstream ss(cell);
      string name;
      ss >> name;
      names.push_back(name);
    }
    cols = names.size();
    csvFile.close();
  }
}

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_):
  file(file_), T(0), cols(0), formatt(formatt_){
  rowFile();
  headers();
  docs.reserve(T);
  csvFile.seekg(0);
  string dropheader, line, cell;
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  while(getline(csvFile, line)){
    stringstream lineS(line);
    BSONObjBuilder dc;
    for (int i = 0; i!=cols; i++){
      getline(lineS, cell, ';');
      istringstream ss(cell);
      if(names[i] == "Date"){
	struct tm tempTM;
	strptime(cell.c_str(), formatt.c_str(), &tempTM);
	dc << "Year" << tempTM.tm_year + 1900
	   << "Month" << tempTM.tm_mon + 1
	   << "Day" << tempTM.tm_mday
	   << "Hour" << tempTM.tm_hour
	   << "Min" << tempTM.tm_min
	   << "Sec" << tempTM.tm_sec
	   << "Wday" << tempTM.tm_wday;
      } else
	dc.append(names[i], cell);
    }
    BSONObj doc = dc.obj();
    docs.push_back(doc);
  }
}

void FXtoBSON::printBSON(){
  for (int i = 0; i != docs.size(); i++){
    cout << docs[i].toString() << endl;
  }
}
