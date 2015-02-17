#include "FXtoBSON.h"

using namespace std;
using namespace mongo;

void FXtoBSON::rowFile(){
  csvFile.seekg(0);
  csvFile.open(file);
  string line;
  while(getline(csvFile, line)){
    T++
      }
  T -= 1;
  csvFile.close();
}


void FXtoBSON::headers(){
  csvFile.seekg(0);
  csvFile.open(file);
  string head, firstline;
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

FXtoBSON::FXtoBSON(const string &file_):file(file_){
  rowFile();
  headers();
  docs.reserve(T);
  csvFile.seekg(0);
  string dropheader, line, cell;
  csvFile.open(file);
  getline(csvFile, dropheader);
  while(getline(csvFile, line)){
    stringstream lineS(line);
    for (int i = 0; i!=cols; i++){
      getline(lineS, cell, ';');
      istringstream ss(cell);
      BSONObjBuilder dc;
      dc.append(names[i], cell);
    }
    BSONObj doc = dc.obj();
    docs.push_back(doc);
  }
}

void FXtoBSON::printBSON(){
  for (int i = 0; i <= docs.size(); i++){
    cout << docs[i].toString() << endl;
  }
}
