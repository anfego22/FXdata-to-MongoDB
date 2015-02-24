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
      names.push_back(cell);
    }
    cols = names.size();
    csvFile.close();
  }
}

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   DBClientConnection &c):
  file(file_), T(0), cols(0), formatt(formatt_), h(0){
  rowFile();
  headers();
  csvFile.seekg(0);
  string dropheader, line, cell;
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  int j = 0;
  while(getline(csvFile, line)){
    stringstream lineS(line);
    BSONObjBuilder quotes, YearPair;
    struct tm tempTM;
    for (int i = 0; i!=cols; i++){
      getline(lineS, cell, ';');
      // This is for convert the string cell to other type
      istringstream ss(cell);
      if(names[i] == "Date")
	strptime(cell.c_str(), formatt.c_str(), &tempTM);
      else {
	double q;
	ss >> q;
	quotes.append(names[i], q);
      }
    }
    YearPair << "Year" << tempTM.tm_year + 1900
	     << "Pair" << "eurusd";
    if(j!=0){
      if(dc.asTempObj()["Year"] == YearPair.asTempObj()["Year"]){
	string min = to_string(tempTM.tm_min);
	bArr.append(min, quotes.obj());
	YearPair.append("Min", bArr.asTempObj());
	dc.appendElementsUnique(YearPair.obj());
      } else {
	docs.push_back(dc.asTempObj());
	string min = to_string(tempTM.tm_min);
	bArr.decouple();
	bArr.append(min, quotes.obj());
	YearPair.append("Min", bArr.asTempObj());
	dc.appendElements(YearPair.obj());
      }
    } else {
      string min = to_string(tempTM.tm_min);
      bArr.append(min, quotes.obj());
      YearPair.append("Min", bArr.asTempObj());
      dc.appendElements(YearPair.obj());
    }
    if(j==T-1){
      docs.push_back(dc.obj());
      bab = bArr.obj();
    }
    j++;
  } // While end;
}

void FXtoBSON::printBSON(){
  for (int i = 0; i != docs.size(); i++){
    cout << docs[i].toString() << endl;
  }
}
