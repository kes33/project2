#include "BTreeNode.h"
#include "BTreeIndex.h"
#include <iostream>
using namespace std;

int main() {

/*
	BTLeafNode* ptr = new BTLeafNode;
	
	//cout << "object is at: " << ptr << endl;
	//cout << "Current number of keys: " << ptr->getKeyCount() << endl;
	
	RecordId rid;
	rid.pid = 0;
	rid.sid = 0;
	
	ptr->insert(5, rid);

	cout << "done with insert of 5 - (0,0)" << endl;
	
	rid.pid=1;
	rid.sid=1;
	ptr->insert(2, rid);

	cout << "done with insert of 2 - (1,1)" << endl << endl;

	ptr->insert(3, rid);

	ptr->insert(4, rid);

	BTLeafNode* sibling = new BTLeafNode;
	int siblingKey;

	ptr-> insertAndSplit(10, rid, *sibling, siblingKey);

	ptr-> insert(11,rid);

	int key;
	if (ptr->readEntry(3, key, rid) != 0) {
		cout << "no entry with that eid in this node" << endl;
	}

	ptr->setNextNodePtr(32);

	ptr->getNextNodePtr();

	delete ptr;
	delete sibling;
*/

	BTreeIndex index;
	index.open("BTreeIndex.idx", 'w');
 
	int i = 0;
	int j = 100;
	RecordId rid;
	while (i < 100) {
		rid.pid = 0;
		rid.sid = i;
		int key = j;

		index.insert(key, rid);
		j--;
		i++;
	}



// TESTING LOCATE
//     IndexCursor cursor;
//     int returnValue = index.locate(5, cursor);
//     cout << "TESTING LOCATE" << endl; 
//     cout << "return value: " << returnValue << endl;
//     cout << "cursor pid: " << cursor.pid << endl;
//     cout << "cursor eid: " << cursor.eid << endl << endl;

// TESTING READFORWARD
//     int key;
//     RecordId rid2;
//     returnValue = index.readForward(cursor, key, rid2);
//     cout << "TESTING READFORWARD" << endl;
//     cout << "return value: " << returnValue << endl;
//     cout << "cursor pid: " << cursor.pid << endl;
//     cout << "cursor eid: " << cursor.eid << endl;
//     cout << "key: " << key << endl;
//     cout << "rid pid: " << rid2.pid << endl;
//     cout << "rid sid: " << rid2.sid << endl << endl;

// TESTING PRINTALLVALUES
//     BTNonLeafNode nln;
//     for (int i = 1; i < 10; i++)
//         nln.insert(2*i, 2*i+1); // add (key,pid) pairs: (0,1), (2,3), (4,5), etc
//     nln.printAllValues();


	index.close();
	
	return 0;


}
