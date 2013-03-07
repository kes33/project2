/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"
#include <vector>
#include <algorithm>
#include <fstream>
#include <set>

using namespace std;

// external functions and variables for load file and sql command parsing 
typedef struct {
  int       key;  // slot number. the first slot is 0
  RecordId  rid;  // page number. the first page is 0
} IndexEntry;

extern FILE* sqlin;
int sqlparse(void);

void getRidsFirstCond(SelCond condition, BTreeIndex& idx, set<RecordId>& resultsToCheck);
void filterKeys(const vector<SelCond>& conds, set<RecordId>& results);

//helper function to compare key conditions (EQ > GT/GE > LT/LE > NE)
bool compareKeyConds (SelCond a, SelCond b) {
  switch (a.comp) {
    case SelCond::EQ:
      return true;
    case SelCond::GE:
    case SelCond::GT:
      if (b.comp == SelCond::EQ)
        return false;
      else
        return true;
    case SelCond::LT:
    case SelCond::LE:
      if (b.comp == SelCond::EQ || b.comp == SelCond::GE || b.comp == SelCond::GT)
        return false;
      else return true;
    default:
      return false;
  }
} 


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}


RC SqlEngine::select(int attr, const string &table, const vector<SelCond>&conds) {

  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    count;
  int    diff;
  bool   indexExists;


  //open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
     fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
     return rc;
  }

  //if conds is empty, scan entire table
  if (conds.empty()) {
    cout << "no conditions, calling linear scan" << endl << endl;
    linearScan(attr, rf, conds);
    rf.close();
    return 0;
  }

  //DOES INDEX EXIST?
  BTreeIndex index;
  indexExists = !(index.open(table + ".idx", 'r') < 0);


  //NO, INDEX DOES NOT EXIST -- scan entire table
  if (!indexExists) {
    cout << "no index available, calling linear scan" << endl << endl;
    linearScan(attr, rf, conds);
    rf.close();
    return 0;
  }

  
  //YES, INDEX EXISTS -- use index to find tuples
    
  //separate conds into keyConds or valueConds
  vector<SelCond> keyConds, valueConds;
  for (int i = 0; i < conds.size(); i++)
    if (conds[i].attr == 1)
      keyConds.push_back(conds[i]);
    else
      valueConds.push_back(conds[i]);

  sort(keyConds.begin(), keyConds.end(), compareKeyConds);  // sort keyConds
  
  cout << endl << "total conds: " << conds.size() << endl;
  cout << "  on key: " << keyConds.size() << endl;
  cout << "  on value: " << valueConds.size() << endl << endl;


  //if keyConds is empty or contains !=, scan entire table
  if (keyConds.empty() || keyConds[0].comp==SelCond::NE) {
    cout << "calling linear scan" << endl;
    linearScan(attr, rf, conds);
    rf.close();
    return 0;
  }

  
  // get results under first condition
  set<RecordId> resultsToCheck;
  getRidsFirstCond(keyConds[0], index, resultsToCheck);
  cout << endl << "size of result set: " << resultsToCheck.size() << endl << endl;
    
  int key;
  string value;
  for (set<RecordId>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
    rf.read(*it, key, value);
    cout << "key: " << key << " value: " << value << endl;
  }
  return 0;

  //if result set is empty after first set, return
  if (resultsToCheck.empty()) {
    cout << "0 tuples found." << endl;
    rf.close();
    return 0;
  }
    
    
    
    
//     //iterate through resultsToCheck
//     set<RecordId>::iterator it;
//     cout << "rids in resultsToCheck after checking first condition (key ";
//     switch (keyConds[0].comp) {
//       case (SelCond::EQ):
//         cout << "== ";
//         break;
//       case (SelCond::GT):
//         cout << "> ";
//         break;
//       case (SelCond::GE):
//         cout << ">= ";
//         break;
//       case (SelCond::LT):
//         cout << "< ";
//         break;
//       case (SelCond::LE):
//         cout << "<= ";
//         break;
//       default:
//         cout << "<>";
//     }
//     cout << " " << keyConds[0].value << ") are: " << endl;
//     for (it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
//       cout << "(" << it->pid << "," << it->sid << ")" << endl;
//     }
// 
//     rf.close();
//     return 0;
}

//finds all of the rid's in the index satisfying the first condition on keys
void getRidsFirstCond(SelCond condition, BTreeIndex& idx, set<RecordId>& resultsToCheck) {    
    IndexCursor cursor;
    RecordId rid;
    int keyInIndex;
    int valueToComp = atoi(condition.value);
    
    switch(condition.comp) {
      //equality condition - look up directly in index
      case (SelCond::EQ):
        cout << "checking on equality of key with " << valueToComp << endl;
        if (idx.locate(valueToComp, cursor)!=0) {
          cout << "on equality check, locate failed to find key greater than or equal to " << valueToComp << endl;
          return;
        }
        if (idx.readForward(cursor, keyInIndex, rid)!=0) {
          cout << "readForward in check on equality failed" << endl;
          return;
        }
        if (valueToComp == keyInIndex){
          cout << "found value for key " << valueToComp << " in index, adding rid to resultsToCheck" << endl;
          resultsToCheck.insert(rid);
        }
        else {
          cout << "in comparing for equality, key not found in index" << endl;
          return;
        }
        break;  
  
      //Greater than or greater than or equal condition
      case (SelCond::GT):
      case (SelCond::GE):
        cout << "checking on GT or GE condition of key compared to " << valueToComp << endl;
        if (idx.locate(valueToComp, cursor) != 0) {
          cerr << "no key in index found with key > or >= " << valueToComp << endl;
          return;
        }
        idx.readForward(cursor, keyInIndex, rid);
        if ((condition.comp==SelCond::GE && keyInIndex==valueToComp) || (condition.comp==SelCond::GT && keyInIndex>valueToComp)){ 
          cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
          resultsToCheck.insert(rid);
        }
        
        //read through remaining rid's in index, with keys greater than current location
        while ((idx.readForward(cursor, keyInIndex, rid))==0) {
          cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
          resultsToCheck.insert(rid);
        }
        break;

      //Less than or less than or equal condition
      case (SelCond::LT):
      case (SelCond::LE):
        cout << "checking on LT or LE condition on key compared to " << valueToComp << endl;
        IndexCursor stoppingCursor;
                  
        //looking for first position in index
        if (idx.locate(0, cursor) != 0) {
          cerr << "no \"smallest\" key found in index" << endl;
          return;
        }
        cout << "cursor of start is (pageId: " << cursor.pid << ", eid: " << cursor.eid << ")" << endl;

        //look for stopping position
                if (idx.locate(valueToComp, stoppingCursor) != 0) {
                    cout << "all keys in index less than " << valueToComp << " - will add all rid's to resultsToCheck" << endl;
                   while (idx.readForward(cursor, keyInIndex, rid)==0) {
            cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
            resultsToCheck.insert(rid);
          }
        }
        else {
          cout << "reading all keys up until stopping cursor: (PageID: " << stoppingCursor.pid << ", eid: " << stoppingCursor.eid << ")" << endl; 
          //read all keys up until stopping point
          IndexCursor nextCursor;
          nextCursor.pid = cursor.pid;
          nextCursor.eid = cursor.eid;
          while (idx.readForward(nextCursor, keyInIndex, rid)==0){
            //cout << "reading forward - reading cursor (pageId: " << cursor.pid << ", eid: " << cursor.eid << ")" << endl;
            if (cursor.pid!=stoppingCursor.pid || cursor.eid!=stoppingCursor.eid) {
              cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
              resultsToCheck.insert(rid);
              cursor.pid = nextCursor.pid;
              cursor.eid = nextCursor.eid;
            }
            else {
              if (condition.comp==SelCond::LE and keyInIndex==valueToComp) {
                cout << "condition is LE, and key with value " << valueToComp << " is in index - including in resultsToCheck" << endl;
                resultsToCheck.insert(rid);       
              }  
              break;
            }
          }
        }  
        break;
    }    
  
}


void filterKeys(const vector<SelCond>& conds, set<IndexEntry>& results)
{
    int key, diff;
    set<IndexEntry> temp;

    // scan through all IndexEntrys in results
    for (set<IndexEntry>::iterator it = results.begin(); it != results.end(); it++) {  
        key = (*it).key;
    
        // check the key against every condition
        for (int i = 0; i < conds.size(); i++) {
    
            // compute the difference between the key value and the condition value
            diff = key - atoi(conds[i].value);

            // skip the key if any condition is not met
            switch (conds[i].comp) {
                case SelCond::EQ:  if (diff != 0) goto next_key;
                case SelCond::NE:  if (diff == 0) goto next_key;
                case SelCond::GT:  if (diff <= 0) goto next_key;
                case SelCond::LT:  if (diff >= 0) goto next_key;
                case SelCond::GE:  if (diff < 0)  goto next_key;
                case SelCond::LE:  if (diff > 0)  goto next_key;
            }
        }

        // all conditions are met -- add IndexEntry to temp set
        temp.insert(*it);

        // move to the next IndexEntry
        next_key:;
    }
    
    results.swap(temp);
}
  

RC SqlEngine::linearScan(int attr, RecordFile &rf, const vector<SelCond>& cond)
{
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
  while (rid < rf.endRid()) {
    // read the tuple
    if ((rc = rf.read(rid, key, value)) < 0) {
      fprintf(stderr, "Error: while reading a tuple from table in linearScan\n");
      goto exit_select;
    }

    // check the conditions on the tuple
    for (unsigned i = 0; i < cond.size(); i++) {
      // compute the difference between the tuple value and the condition value
      switch (cond[i].attr) {
      case 1:
  diff = key - atoi(cond[i].value);
  break;
      case 2:
  diff = strcmp(value.c_str(), cond[i].value);
  break;
      }

      // skip the tuple if any condition is not met
      switch (cond[i].comp) {
      case SelCond::EQ:
  if (diff != 0) goto next_tuple;
  break;
      case SelCond::NE:
  if (diff == 0) goto next_tuple;
  break;
      case SelCond::GT:
  if (diff <= 0) goto next_tuple;
  break;
      case SelCond::LT:
  if (diff >= 0) goto next_tuple;
  break;
      case SelCond::GE:
  if (diff < 0) goto next_tuple;
  break;
      case SelCond::LE:
  if (diff > 0) goto next_tuple;
  break;
      }
    }

    // the condition is met for the tuple. 
    // increase matching tuple counter
    count++;

    // print the tuple 
    switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

    // move to the next tuple
    next_tuple:
    ++rid;
  }

  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  string tableName = table + ".tbl";
  string indexName = table + ".idx";
  
  //open loadfile as fstream
  ifstream loadStream;
  loadStream.open(loadfile.c_str());
  if (loadStream.fail()) {
    cerr << "Error opening loadfile." << endl;
    return(RC_FILE_OPEN_FAILED);
  }


  //if table exists but index does not, don't create index
  ifstream table_file(tableName.c_str());
  ifstream index_file(indexName.c_str());
  if (table_file.good() && !index_file.good())
    index = false;


  //open RecordFile - if file does not already exist, is created 
  RecordFile records;
  if (records.open(tableName, 'w')!=0) {
    cerr << "Error opening record file." << endl;
    return(RC_FILE_OPEN_FAILED);
  }
  
  
  //open index file
  BTreeIndex idx;
  if (index) {
    if (idx.open(indexName, 'w') != 0) {
      cerr << "Error opening index file." << endl;
      return (RC_FILE_OPEN_FAILED);
    }
  }


  //read in records from loadfile
  string value, recordLine;
  while (getline(loadStream, recordLine)) {
    int key;
    RecordId rid;
    
//    cout << "recordLine is " << recordLine << endl;    

    //parse load line:
    if (parseLoadLine(recordLine, key, value)!=0) {
      cerr << "Error parsing load line." <<endl ;
      return(RC_INVALID_FILE_FORMAT);
    }  
    
    //store in recordfile:
    if (records.append(key, value, rid)!=0) {
      cerr<< "Error appending to records file." <<endl;
      return(RC_FILE_WRITE_FAILED);
    }
 
    //store in index if index is selected
    if (index) {
      if (idx.insert(key, rid)!=0) {
        cerr << "Error inserting value in index in SqlEngine.load()" << endl;
        return(RC_FILE_WRITE_FAILED);
      }
    }  
  //  cout << "Storing line number " <<count << ": key-" << key << " | value-" << value << endl; 
  }
  
  if (index)
    idx.close();
  records.close();
  loadStream.close();
    return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
