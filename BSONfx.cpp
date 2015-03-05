#include "FXtoBSON.h"

using namespace std;
using namespace mongo;

void FXtoBSON::headers(){
  if(names.size() == 0){
    csvFile.seekg(0);
    csvFile.open(file.c_str());
    string cell, firstline;
    getline(csvFile, firstline);
    stringstream lineS(firstline);
    while(getline(lineS, cell, sep)){
      names.push_back(cell);
    }
    cols = names.size();
    csvFile.close();
  }
}

void FXtoBSON::rowFile(){
  if(rows == 0){
    csvFile.seekg(0);
    csvFile.open(file.c_str());
    string line, cell;
    getline(csvFile, line);// Drop Headers
    getline(csvFile, line);
    istringstream ss(line);
    for(int i = 0; i!= cols; i++){
      getline(ss, cell, sep);
      if(names[i] == "Date")
	strptime(cell.c_str(), formatt.c_str(), &time0);
    }
    while(getline(csvFile, line)){
      rows++;
    }
    rows += 1; // Headers not included
    csvFile.close();
  }
}

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, DBClientConnection &c, const char &sep_):
  file(file_), cols(0), rows(0), formatt(formatt_), sep(sep_){
  db = "FOREX.";
  db += pair;
  string dropheader, line, cell;
  int j = 1;
  struct tm tempTM;
  headers();
  rowFile();
  csvFile.seekg(0);
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  BSONObjBuilder hour;
  for(int i = 0; i< 60; i++){
    hour.append(to_string(i), 0);
  }
  HOUR = hour.obj();
  while(getline(csvFile, line)){
    string query;
    BSONObjBuilder quotes, FIND;
    istringstream lineS(line);
    for (int i = 0; i!=cols; i++){
      getline(lineS, cell, sep);
      if (names[i] == "Date")
	strptime(cell.c_str(), formatt.c_str(), &tempTM);
      else{
	istringstream td(cell);
	double d;
	td >> d;
	quotes.append(names[i], d);
      }
    }
    BSONObj QUOTE = quotes.obj();
    string min = to_string(tempTM.tm_min);
    string op, hi, lo, cl, vl;
    op = string("Open.") + min;
    hi = string("High.") + min;
    lo = string("Low.") + min;
    cl = string("Close.") + min;
    vl = string("Vol.") + min;
    if(time0.tm_hour == tempTM.tm_hour){
      time0.tm_min = 0;
      FIND.appendTimeT("Date", timegm(&time0)); 
    } else {
      tempTM.tm_min = 0;
      FIND.appendTimeT("Date", timegm(&tempTM));
    }
    try{
      auto_ptr<DBClientCursor> cursor = c.query(db, FIND.asTempObj());
      if(cursor->more()){
	c.update(db, FIND.obj(),
		 BSON("$set" << 
		      BSON(op << QUOTE.getField("Open").numberDouble() <<
			   hi << QUOTE.getField("High").numberDouble() <<
			   lo << QUOTE.getField("Low").numberDouble() <<
			   cl << QUOTE.getField("Close").numberDouble() <<
			   vl << QUOTE.getField("Vol").numberDouble())));
      } else {
	c.update(db, FIND.asTempObj(),
		 BSON("$set" <<BSON("Open" << HOUR <<
				     "High" << HOUR <<
				     "Low" << HOUR <<
				     "Close" <<HOUR <<
				     "Vol" << HOUR)), true);
	c.update(db, FIND.obj(),
		 BSON("$set" <<
		      BSON(op << QUOTE.getField("Open").numberDouble() <<
			   hi << QUOTE.getField("High").numberDouble() <<
			   lo << QUOTE.getField("Low").numberDouble() <<
			   cl << QUOTE.getField("Close").numberDouble() <<
			   vl << QUOTE.getField("Vol").numberDouble())));
      }
    } catch( const mongo::DBException &e) {
      std::cout << "caught " << e.what() << std::endl;
    }
    time0 = tempTM;
  } // While end;
}


