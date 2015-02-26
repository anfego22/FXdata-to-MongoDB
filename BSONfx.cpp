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

FXtoBSON::FXtoBSON(const string &file_, const string &formatt_,
		   const string &pair, DBClientConnection &c, const char &sep_):
  file(file_), cols(0), formatt(formatt_), h(-1), sep(sep_){
  db = "FOREX.";
  db += pair;
  string dropheader, line, cell;
  int j = 0;
  struct tm tempTM;
  headers();
  //Years();
  csvFile.seekg(0);
  csvFile.open(file.c_str());
  getline(csvFile, dropheader);
  BSONObjBuilder Open, High, Low, Close, Vol;
  while(getline(csvFile, line)){
    string query;
    BSONObjBuilder FIND;
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
    string min = to_string(tempTM.tm_min);
    string hour = to_string(tempTM.tm_hour);
      if(h != tempTM.tm_hour | j == T-1){
	tempTM.tm_min = 0;
	FIND.appendTimeT("Date", mktime(&tempTM)); 
	try{
	  c.update(db, FIND.obj(),
		   BSON("$addToSet" << BSON("Open" << Open.obj() <<
					    "High" << High.obj() <<
					    "Low" << Low.obj() <<
					    "Close" << Close.obj() <<
					    "Vol" << Vol.obj())), true);
	} catch( const mongo::DBException &e) {
	  std::cout << "caught " << e.what() << std::endl;
	}
      } else {
	BSONObj QUOTE = quotes.obj();
	Open.append(min, QUOTE.getField("Open").numberDouble());
	High.append(min, QUOTE.getField("High").numberDouble());
	Low.append(min, QUOTE.getField("Low").numberDouble());
	Close.append(min, QUOTE.getField("Close").numberDouble());
	Vol.append(min, QUOTE.getField("Vol").numberDouble());
      }
      h = tempTM.tm_hour;
      j++;
  } // While end;
}

