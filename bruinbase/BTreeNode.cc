#include "BTreeNode.h"
#include <iostream>
#include <cstring>
using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
//NOTE: returns RC_FILE_READ_FAILED on error
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	// clear buffer before reading into it
	memset(buffer, '\0', PageFile::PAGE_SIZE);
	 
	if (pf.read(pid, (void*)buffer) != 0) {
		cerr << "Error on BTLeafNode read from PageFile" << endl;
		exit(RC_FILE_READ_FAILED);
	}

	return 0; }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
//returns RC_FILE_WRITE_FAILED on error
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	RC error;
	error = pf.write(pid, buffer);
	if (error !=0) {
		//cerr << "Error on writing BTLeafNode to PageFile" << endl; 
		return(RC_FILE_WRITE_FAILED);
	}

	return 0; 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ return *(int*)buffer; }

//set the keyCount variable
void BTLeafNode::setKeyCount(int value) {
	*(int*)buffer = value;
}


/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
//assuming node is in buffer - will need to write back out to buffer after insertion!
//returns RC_NODE_FULL if node is full
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	//check if node is full
	int numKeys = getKeyCount();

	if (numKeys == NUMNODEPTRS-1){
		//cerr << "Node is full - cannot insert" << endl;
		return RC_NODE_FULL;
	}

	else return insertInBuffer(key, rid);
}


/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
//NOTE: adds a recordID and key to a full node - so the value of NUMNODEPTRS cannot 
//be such that adding another recordId and key would overflow the page
//NOTE: responsability of getting and setting the nextNodePtrs is on the calling function
//NOTE: writes to siblings buffer - caller will need to write to pageFile
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	struct leafNode {
        RecordId rid;
        int key;
    };

	//cout << "node's buffer currently reads: ";
	//printNode();
	//cout << endl;

	//find where new key should go
	int temp;
	char* placement = bufPlacement(buffer, key, temp);
	//cout << "insert into buffer should be at: " << (void*) placement << endl;

	//insert key in buffer 
	insertInBuffer(key, rid);

	//find split point - ptr will point to first location to be copied to sibling
	struct leafNode* ptr = (struct leafNode*)(buffer+sizeof(int));
	int numOnLeft = (NUMNODEPTRS+1)/2;
	//cout << "number of keys staying on left is " << numOnLeft << endl;
	int i;
	for (i=0; i < numOnLeft; i++) {
		ptr++;
	}

	//calculate how much to copy to sibling
	int filled = (sizeof(RecordId) + sizeof(int)) * (getKeyCount()+1) + sizeof(PageId) + sizeof(int); 
	int amtToCopy = filled - ((char*)ptr-(char*)buffer);

	//copy all to sibling (including nextNodePtr)
	memcpy((void*)(sibling.buffer + sizeof(int)), (void*)ptr, amtToCopy);

	//change keyCount of leafNode
	setKeyCount(numOnLeft);
	
	//give sibling its keyCount - are NUMNODEPTRS keys in total now, after insertion
	sibling.setKeyCount(NUMNODEPTRS - numOnLeft);

	//remove rest of buffer
	memset(ptr, '\0', amtToCopy);

	//cout << "buffer now holds ";
 	//printNode();
	//cout << endl;

	//cout << "sibling now holds: ";
    //sibling.printNode(sibling.buffer);
    //cout << endl;

	//store the value of the sibling key in siblingKey
	char* ptr2 = sibling.buffer + sizeof(int);
	ptr2 += sizeof(RecordId);
	siblingKey = *(int*)ptr2;

	return 0; }

/*
 * Find the entry whose key value is larger than or equal to searchKey
 * and output the eid (entry number) whose key value >= searchKey.
 * Remeber that all keys inside a B+tree node should be kept sorted.
 * @param searchKey[IN] the key to search for
 * @param eid[OUT] the entry number that contains a key larger than or equalty to searchKey
 * @return 0 if successful. Return an error code if there is an error.
 */
//NOTE: if error (key >= searchKey not found), returns RC_NO_SUCH_RECORD
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
	int temp;
	
	//find location where searchKey should go, using bufPlacement 
	bufPlacement(buffer, searchKey, temp);

	//cout << "in locate, eid would be " << temp << endl;

	//if entry not found - return error code
	if (temp == -1) {
		return RC_NO_SUCH_RECORD;
	}

	else eid = temp;
	return 0; 
}


/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
//NOTE: returns RC_NO_SUCH_RECORD if eid is not valid in given node
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	if (eid > getKeyCount()) {
		return RC_NO_SUCH_RECORD;
	}

	//go to location in buffer specified by eid
	char* entry = goToEid(buffer, eid);
	
	//copy RecordId into rid
	memcpy((void*)&rid, (void*)entry, sizeof(RecordId));
	//cout << "in readEntry, eid of " << eid << " is (" << rid.pid << ", " << rid.sid << ") | ";

	entry += sizeof(RecordId);

	//copy key 
	key = *(int*)entry;
	//cout << "key:" << key << endl;
	
	return 0; }

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	//go to final RecordId/Key pair in node
	char* ptr = goToEid(buffer, getKeyCount()-1);

	ptr+= (sizeof(RecordId)+sizeof(int));
	//cout << "to read nextNodePtr, ptr is at: " << (void*)ptr << endl;
	//cout << "next node pointer is " << *(PageId*)ptr << endl;

	return *(int*)ptr; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	//go to final RecordId/Key pair in node
	char* ptr = goToEid(buffer, getKeyCount()-1);
	ptr+= (sizeof(RecordId)+sizeof(int));
	
	*(int*)ptr = pid;

	//cout << "after setting nextNodePtr, buffer now reads: ";
	//printNode();
	//cout << endl;

	return 0; 
}

/*------------*/
//helper functions:

//printNode helper function
void BTLeafNode::printNode() {
	struct leafNode {
		RecordId rid;
		int key;
	};

	struct leafNode* ptr = (struct leafNode*)(buffer + sizeof(int));
	//cout << "in printNode, buffer is at: " << (void*)buffer << endl;
	//cout << "in printNode, ptr starts at: " << (void*)ptr << endl;

	int count;
	int keyCount = getKeyCount();

	//check if node empty
	if (keyCount == 0) {
		//cout << "Node is empty." << endl;
		return;
	}

	else {
		//cout << "There are " << keyCount << " keys in the node." << endl;
		for (count = 0; count < keyCount; count++) {
			//cout << "rid:(" << (ptr->rid.pid) << "," << (ptr->rid.sid) << ") | key: " << (ptr->key) << endl;
			ptr++;
		}
		//cout << "pageid: " << *(PageId*)ptr << endl;	
	}
}

//find place in sorted key order for new entry in node
//eid stores the location of the first node greater than or equal to the key
//if no such key is found, returns -1
char* BTLeafNode::bufPlacement (const char* buf, int k, int& eid) {
	struct leafNode {
		RecordId rid;
		int key;
	};

 	struct leafNode* ptr = (struct leafNode*)(buf+sizeof(int));

	//cout << "in bufPlacement, buffer is at: " << (void*)buf << endl;
	//cout << "in bufPlacement, placement starts at: " <<	(void*)ptr << endl;
	
	//after exiting loop, either we have found the first key larger than or equal to 
	//the sought value and we should insert key before it
	//or no such key exists, in which case ptr stops at the end of all current keys 
	int count = 0;
	int numKeys = getKeyCount();
	while ((count < numKeys) && (ptr->key < k)){
		ptr++;
		count++;
	}

	if (count == numKeys) {
		eid = -1;
	}

	else eid = count;

	return (char*)ptr;
}	

//helper function to go to location in buffer indicated by eid 
//assumes eid is valid in given buffer
char* BTLeafNode::goToEid (char* buffer, int eid)
{
	//skip over the numKeys info at top of buffer
	char* ptr = buffer + sizeof(int); 

	//jump over unwanted keys in buffer 
	ptr += (sizeof(RecordId)+sizeof(int))*eid;

	return ptr;
}

//helper function used by both insert and insertAndSplit
RC BTLeafNode::insertInBuffer(int key, const RecordId& rid) {

	//cout << "node's buffer currently reads: ";
	//printNode();
	//cout << endl;

	//find where new key should go
	int temp1;
	char* placement = bufPlacement(buffer, key, temp1);

	//find how far into buffer placement is
	int prePlacement = placement - buffer;
	int filled = (sizeof(RecordId) + sizeof(int)) * getKeyCount() + sizeof(PageId)+ sizeof(int);
	//cout << "amount before placement pointer is: " << prePlacement << endl;
	//cout <<"placement is at: " << (void*)placement << endl;
	//cout <<"amount of buffer filled: " << filled << endl;

	//copy rest of buffer into a temp buffer
	char temp[PageFile::PAGE_SIZE];
	int amtToCopy = (filled - prePlacement);
	//cout << "amount to copy is: " << amtToCopy << endl;
	memcpy((void*)temp, (const void*)placement, amtToCopy);

	//insert key into buffer
	memcpy((void*)placement, (const void*) &rid, sizeof(rid));
	placement += sizeof(rid);
	//cout << "after rid insertion, placement is at: " << (void*)placement << endl;
	*((int*)placement) = key;
	placement += sizeof(int);
	//cout << "after key insertion, placement is at: " << (void*)placement << endl;
	
	//copy contents of temp back into buffer
	memcpy((void*)placement, (const void*)temp, amtToCopy); 

	//increment the keyCount variable
	setKeyCount(getKeyCount()+1);
	
	//cout << "after insert, buffer is now: ";
	//printNode();
	//cout << endl;

	return 0; 
}

//----------------------------------------------------------------------------

////////////////////////////////////////////////////////////////////////
///////////////////////////  TESTING  //////////////////////////////////
// void BTNonLeafNode::test() {
//     int * test = buffer + 1 + keyCount * 2;
//     *test = 5;
// }
// 
// void BTNonLeafNode::printAllValues() {
//     int * index = buffer;
// 
//     cout << "PRINTING NON-LEAF NODE DATA"          << endl;
//     cout << "  member variables: "                 << endl;
//     cout << "    keyCount: " << keyCount           << endl;
//     cout << "  buffer contents: "                  << endl;
//     cout << "    Count: " << *(index)              << endl;
//     cout << "    Key: -- \tPID: " << *(++index)    << endl;
//     
//     int count = 0;
//     while (count++ < keyCount) {
//        cout << "    Key: " << *(++index);
//        cout << "\tPID: " << *(++index) << endl;
//    }
// }
////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf) {
    // clear buffer before reading into it
    memset(buffer, 0, PageFile::PAGE_SIZE);

    // read page identified by pid into buffer 
    RC rc = pf.read(pid, buffer);
    if (rc != 0)
        return rc;

    // get total number of keys in node
    keyCount = *buffer;
    return 0;
}

/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf) {
    // write buffer into page identified by pid
    return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
    return keyCount;
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
    // check if node has room to add key/pid pair
    if (getKeyCount() == KEYS_PER_NODE) {
        //cout << "BTNonLeafNode.insert(): error adding (key=" << key << ",pid=" << pid << ") to node; node full" << endl;
        return RC_NODE_FULL;
    }

    return insertKey(key, pid);
}


RC BTNonLeafNode::insertKey(int key, PageId pid) {
    // position of first PageId/key pair
    int * keyPos = buffer;
	keyPos += 2;
    int * pidPos = keyPos + 1;

    if (keyCount != 0) {
        // find positions where PageId/key should be inserted
        int pairIndex = 0;
        while (key > *keyPos && pairIndex < keyCount) {
			keyPos += 2;
            pidPos += 2;
            pairIndex++;
        }

        // shift all PageId/key pairs down to make room for new pair
        int bytesToCopy = PageFile::PAGE_SIZE - ((char*)keyPos-(char*)buffer) - 2*sizeof(int);
		char temp[bytesToCopy];
        memcpy((void*)temp, (void*)keyPos, bytesToCopy);
		int* newPos = keyPos+2;
        memcpy((void*)newPos, (void*)temp, bytesToCopy);
        
//         cout << "inserting (key=" << key << ",pid=" << pid << ")" << endl;
//         cout << "buffer pos: " << buffer << endl;
//         cout << "key pos: " << keyPos << endl;
//         cout << "pid pos: " << pidPos << endl;
//         cout << "new pos: " << newPos << endl;
//         cout << "bytesToCopy: " << bytesToCopy << endl;
//         cout << "end of buffer: " << (int*)(bytesToCopy + (char*)newPos) << endl;
        
    }

    // write pid and key
    *keyPos = key;
    *pidPos = pid;
    *buffer = ++keyCount;
    return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{
    // insert (key, pid) pair into this node
    insertKey(key, pid);
    
    //cout << "BTNonLeafNode.insertAndSplit: (key,pid) (" << key << "," << pid << ") inserted, printing all values before split" << endl;
    //printAllValues();
    
    // update key counts
    keyCount = keyCount / 2;
    if (keyCount % 2 == 0)
        sibling.keyCount = keyCount - 1;
    else
        sibling.keyCount = keyCount;

    *buffer = keyCount;
    *(sibling.buffer) = sibling.keyCount;

    // return middle key
    midKey = *(buffer + 2 + 2*keyCount);    // note: must skip key count and first pid

    // calculate bytes going to each sibling
    int bytesToKeep = (2 + 2*keyCount) * sizeof(int);
    int bytesToSend = PageFile::PAGE_SIZE - (bytesToKeep + sizeof(int));    // note: need to also send pid of midKey

    // copy (key, pid) pairs to sibling
    memset(sibling.buffer+1, 0, PageFile::PAGE_SIZE-sizeof(int));   // note: erase all but keyCount
    memcpy(sibling.buffer+1, buffer + bytesToKeep/sizeof(int) + 1, bytesToSend);

    // remove copied pairs from this node
    memset(buffer + bytesToKeep/sizeof(int), 0, PageFile::PAGE_SIZE - bytesToKeep);

    return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
    // pointer to first key
    int * pos = buffer + 2;

    // look through array for first entry whose key is >= searchKey
    int count = 0;
    while (searchKey >= *pos && count < keyCount) {
        pos += 2;
        count++;
    }

    // return pid
    pid = *(pos - 1);
    return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
    // set pid1
    int * pos = buffer + 1;
    *pos = pid1;
    
    // set key
    pos++;
    *pos = key;

    // set pid2
    pos++;
    *pos = pid2;

    // update keyCount
    keyCount = 1;
    *buffer = keyCount;

    return 0;
}



