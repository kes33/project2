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


bool operator<(const IndexEntry& a, const IndexEntry& b) {
	return (a.rid<b.rid);
}

extern FILE* sqlin;
int sqlparse(void);

void getRidsFirstCond(SelCond condition, BTreeIndex& idx, set<IndexEntry>& resultsToCheck);
void filterKeys(const vector<SelCond>& conds, set<IndexEntry>& results);
bool valueSatisfiesConds(string& value, const vector<SelCond>& conds);

//helper function to compare key conditions (EQ > GT/GE > LT/LE > NE)
bool compareKeyConds (const SelCond& a, const SelCond& b) {
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
    bool   indexExists;
    int    key;
    string value;
    

    // OPEN FILES
    // open the table file
    if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
        //fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
        return rc;
    }


    // open the index file -- if it doesn't exist, perform linearScan
    BTreeIndex index;
    if (index.open(table + ".idx", 'r') < 0) {
        //cout << "no index available, calling linear scan" << endl << endl;
        linearScan(attr, rf, conds);
        rf.close();
        return 0;
    }

	vector<SelCond> keyConds, valueConds;

    // PREPARE SELECT CONDITIONS
    // separate conds into keyConds (sorted) or valueConds
    // scan entire table if no conditions
    if (conds.empty()) {
        if (attr == 2 || attr == 3) {  // value or *	
		    //cout << "no conditions, calling linear scan" << endl << endl;
            linearScan(attr, rf, conds);
            rf.close();
            return 0;
        }
        else {
		    SelCond condition;
		    condition.comp=SelCond::GE;
		    condition.attr=1;
            char condValue = '1';
		    condition.value = &condValue;
    	    keyConds.push_back(condition);
        }
	}

    for (int i = 0; i < conds.size(); i++)
        if (conds[i].attr == 1)
            keyConds.push_back(conds[i]);
        else
            valueConds.push_back(conds[i]);

    sort(keyConds.begin(), keyConds.end(), compareKeyConds);

    // if keyConds is empty or contains only <> select conditions, scan entire table
    if (keyConds.empty() || keyConds[0].comp==SelCond::NE) {
        //cout << "calling linear scan" << endl;
        linearScan(attr, rf, conds);
        rf.close();
        return 0;
    }

  
    // GET KEYS
    // get keys that satisfy all key conditions
    //    first, apply first condition
    //    then filter those results with remaining conditions
    set<IndexEntry> resultsToCheck;
    getRidsFirstCond(keyConds[0], index, resultsToCheck);
    
    if (resultsToCheck.empty()) {
        //cout << "0 tuples found." << endl;
        rf.close();
        return 0;
    }
    else
        filterKeys(keyConds, resultsToCheck);


    // PRINT TUPLES
    // no valueConds to check -- proceed to print tuples
    if (valueConds.empty()) {
        switch (attr) {
    
            case 1:    // print key
                for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++)
                    cout << it->key << endl;
                break;
      
            case 2:    // print value
                for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                    rf.read(it->rid, key, value);
                    cout << value << endl;
                }
                break;
    
            case 3:    // print key and value
                for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                    rf.read(it->rid, key, value);
                    cout << key << "\t" << value << endl;
                }
                break;
      
            case 4:    // print count
                cout << resultsToCheck.size() << endl;
                break;
            }
    }
  
    // there are value conds -- apply them before printing tuples
    else {
        switch (attr) {
    
        case 1:    // print key if value satisfies valueConds
            for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                rf.read(it->rid, key, value);
                if (valueSatisfiesConds(value, valueConds))
                    cout << it->key << endl;
            }
            break;
      
        case 2:    // print value if it satisfies valueConds
            for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                rf.read(it->rid, key, value);
                if (valueSatisfiesConds(value, valueConds))
                    cout << value << endl;
            }
            break;
    
        case 3:    // print key and value if value satisfies valueConds
            for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                rf.read(it->rid, key, value);
                if (valueSatisfiesConds(value, valueConds))
                    cout << key << "\t" << value << endl;
            }
            break;
      
        case 4:    // print count of values that satisfy valueConds
            int count = 0;
            for (set<IndexEntry>::iterator it = resultsToCheck.begin(); it != resultsToCheck.end(); it++) {
                rf.read(it->rid, key, value);
                if (valueSatisfiesConds(value, valueConds))
                    count++;
            }
            cout << count << endl;
            break;
        }
    }
  
  
    // CLEAN UP
    rf.close();
    return 0;
}

bool valueSatisfiesConds(string& value, const vector<SelCond>& conds) {

    // check the value against every condition
    int diff;
    for (int i = 0; i < conds.size(); i++) {

        // compute the difference between the input value and the condition value
        diff = strcmp(value.c_str(), conds[i].value);

        // return false if any condition is not met
        switch (conds[i].comp) {
            case SelCond::EQ:  if (diff != 0) return false;
            break;
            case SelCond::NE:  if (diff == 0) return false;
            break;
            case SelCond::GT:  if (diff <= 0) return false;
            break;
            case SelCond::LT:  if (diff >= 0) return false;
            break;
            case SelCond::GE:  if (diff < 0)  return false;
            break;
            case SelCond::LE:  if (diff > 0)  return false;
            break;
        }
    }

    // all conditions are met
    return true;
}


//finds all of the rid's in the index satisfying the first condition on keys
void getRidsFirstCond(SelCond condition, BTreeIndex& idx, set<IndexEntry>& resultsToCheck) {    
    IndexCursor cursor;
    RecordId rid;
    int keyInIndex;
	IndexEntry idxEntry;
	bool continueLoop;
	RC error;
    int valueToComp = atoi(condition.value);
    
    switch(condition.comp) {
      //equality condition - look up directly in index
      case (SelCond::EQ):
        //cout << "checking on equality of key with " << valueToComp << endl;
        if (idx.locate(valueToComp, cursor)!=0) {
          //cout << "on equality check, locate failed to find key greater than or equal to " << valueToComp << endl;
          return;
        }
        if (idx.readForward(cursor, keyInIndex, rid)!=0) {
          //cout << "readForward in check on equality failed" << endl;
          return;
        }
        if (valueToComp == keyInIndex){
          //cout << "found value for key " << valueToComp << " in index, adding rid to resultsToCheck" << endl;
          idxEntry.rid = rid;
		  idxEntry.key = keyInIndex;
		  resultsToCheck.insert(idxEntry);
        }
        else {
          //cout << "in comparing for equality, key not found in index" << endl;
          return;
        }
        return;  
  
      //Greater than or greater than or equal condition
      case (SelCond::GT):
      case (SelCond::GE):
        //cout << "checking on GT or GE condition of key compared to " << valueToComp << endl;
        if (idx.locate(valueToComp, cursor) != 0) {
          //cerr << "no key in index found with key > or >= " << valueToComp << endl;
          return;
        }
        
		//check first value for GE or GT condition
		error = idx.readForward(cursor, keyInIndex, rid);
        if ((condition.comp==SelCond::GE && keyInIndex==valueToComp) || (condition.comp==SelCond::GT && keyInIndex>valueToComp)){ 
          //cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
          idxEntry.rid = rid;
          idxEntry.key = keyInIndex;
		  resultsToCheck.insert(idxEntry);
        }
        
		if (error !=0 )   //the retrieved key was the last one in the tree - we're done
			return;

		//more to read - read through remaining rid's in index, with keys greater than current location
        continueLoop = true;
		while (continueLoop) {
				error = idx.readForward(cursor, keyInIndex, rid); 
          		//cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
          		idxEntry.rid = rid;
          		idxEntry.key = keyInIndex;
		 	 	resultsToCheck.insert(idxEntry);
				if (error != 0)
					continueLoop = false;
        }
        return;


      //Less than or less than or equal condition
      case (SelCond::LT):
      case (SelCond::LE):
        //cout << "checking on LT or LE condition on key compared to " << valueToComp << endl;
        IndexCursor stoppingCursor;
                  
        //looking for first position in index
        if (idx.locate(0, cursor) != 0) {
          //cerr << "no \"smallest\" key found in index" << endl;
          return;
        }
        //cout << "cursor of start is (pageId: " << cursor.pid << ", eid: " << cursor.eid << ")" << endl;

        //look for stopping position
        if (idx.locate(valueToComp, stoppingCursor) != 0) {
           	//cout << "all keys in index less than " << valueToComp << " - will add all rid's to resultsToCheck" << endl;
            continueLoop = true;
			while (continueLoop) {
				error = idx.readForward(cursor, keyInIndex, rid);
            	//cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
            	idxEntry.rid = rid;
				idxEntry.key = keyInIndex;
				resultsToCheck.insert(idxEntry);
				if (error != 0)
					continueLoop = false;
          	}
			return;
        }

        else {
          //cout << "reading all keys up until stopping cursor: (PageID: " << stoppingCursor.pid << ", eid: " << stoppingCursor.eid << ")" << endl; 
          //read all keys up until stopping point
          IndexCursor nextCursor;
          nextCursor.pid = cursor.pid;
          nextCursor.eid = cursor.eid;
          while (idx.readForward(nextCursor, keyInIndex, rid)==0){
            //cout << "reading forward - reading cursor (pageId: " << cursor.pid << ", eid: " << cursor.eid << ")" << endl;
            if (cursor.pid!=stoppingCursor.pid || cursor.eid!=stoppingCursor.eid) {
              //cout << "inserting into resultsToCheck rid (" << rid.pid << "," << rid.sid <<") with key " << keyInIndex << endl;
              idxEntry.rid = rid;
	 		  idxEntry.key = keyInIndex;
			  resultsToCheck.insert(idxEntry);
              cursor.pid = nextCursor.pid;
              cursor.eid = nextCursor.eid;
            }
            else {
              if (condition.comp==SelCond::LE and keyInIndex==valueToComp) {
                //cout << "condition is LE, and key with value " << valueToComp << " is in index - including in resultsToCheck" << endl;
                idxEntry.rid = rid;
				idxEntry.key = keyInIndex;
				resultsToCheck.insert(idxEntry);       
              }
   			  break;
			}
		  }
		return;
	   }

	}  
}


// filters a given keys with given conditions in place
void filterKeys(const vector<SelCond>& conds, set<IndexEntry>& results)
{
    // return immediately if no conditions to check
    if (conds.empty())
      return;

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
                break;
                case SelCond::NE:  if (diff == 0) goto next_key;
                break;
                case SelCond::GT:  if (diff <= 0) goto next_key;
                break;
                case SelCond::LT:  if (diff >= 0) goto next_key;
                break;
                case SelCond::GE:  if (diff < 0)  goto next_key;
                break;
                case SelCond::LE:  if (diff > 0)  goto next_key;
                break;
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
      //fprintf(stderr, "Error: while reading a tuple from table in linearScan\n");
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
    //cerr << "Error opening loadfile." << endl;
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
    //cerr << "Error opening record file." << endl;
    return(RC_FILE_OPEN_FAILED);
  }
  
  
  //open index file
  BTreeIndex idx;
  if (index) {
    if (idx.open(indexName, 'w') != 0) {
      //cerr << "Error opening index file." << endl;
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
      //cerr << "Error parsing load line." <<endl ;
      return(RC_INVALID_FILE_FORMAT);
    }  
    
    //store in recordfile:
    if (records.append(key, value, rid)!=0) {
      //cerr<< "Error appending to records file." <<endl;
      return(RC_FILE_WRITE_FAILED);
    }
 
    //store in index if index is selected
    if (index) {
      if (idx.insert(key, rid)!=0) {
        //cerr << "Error inserting value in index in SqlEngine.load()" << endl;
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
