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
  file(file_), T(0), cols(0), formatt(formatt_){
  
  rowFile();
  headers();
  docs.reserve(T);
  csvFile.seekg(0);
  string dropheader, line, cell;
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  BSONArrayBuilder bArr;
  int j = 0;
  while(getline(csvFile, line)){
    stringstream lineS(line);
    BSONObjBuilder quotes, YearPair;
    //BSONObjBuilder Month, Day, Hour, Min;
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
    //string minute = std::to_string(tempTM.tm_min);
    if(j!=0){
      if(dc.obj()["Year"] == YearPair.obj()["Year"]){
	bArr.append(quotes.obj());
	YearPair.appendArray("Min", bArr.arr());
	dc.appendElementsUnique(YearPair.obj());
      }
      else {
	docs.push_back(dc.obj());
	dc.appendElements(YearPair.obj());
      }
    } else {
      bArr.append(quotes.obj());
      YearPair.appendArray("Min", bArr.arr());
      dc.appendElements(YearPair.obj());
    }
    j++;
  } // While end;
}

void FXtoBSON::printBSON(){
  for (int i = 0; i != docs.size(); i++){
    cout << docs[i].toString() << endl;
  }
}
