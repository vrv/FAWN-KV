/* -*- Mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
using namespace std;

#include <list>
#include <map>
#include <iostream>
#include <pthread.h>


// Code stolen from http://www.codeproject.com/KB/applications/cpp_mru_cache.aspx

/**
 * MRU Cache
 *
 * contains up to a fixed size of "Most Recently Used" items, where items are assiciated with keys.
 * when asked to fetch a value that is not in the cache, HandleNonExistingKeyFetch is called.
 * when an item is removed from the cache, HandleItemRelease is called.
 * implementor of this cache must provide those methods.
 *
 */

class MruCache
{
public:
    
    const int maxLength;

    MruCache(int iMaxLength) : maxLength(iMaxLength) {
      pthread_mutex_init(&cache_mutex, NULL);
    }

    string* FetchItem(const string &key) { return __fetch_item(key); }

    void Invalidate(const string &key) { __evict_item(key);}

    void AddItem(const string &key, string* value) { __add_item(key, value);}

    virtual ~MruCache() { 
      pthread_mutex_destroy(&cache_mutex);
      Clear(); 
    }

    virtual void Clear() { __clear(); }


protected:

    virtual void HandleItemRelease(string* key, string* value) { };

    virtual string* HandleNonExistingKeyFetch(string* key) = 0;

private:

    typedef struct _Entry
    {
        string* key;
        string* value;
    } Entry;

    typedef std::list<Entry> EntryList;
    EntryList listOfEntries;

    /**
     * for fast search in the cache. this map contains pointers to iterators in EntryList.
     */
    typedef std::map<string, void*> ItrPtrMap;
    ItrPtrMap mapOfListIteratorPtr;

    pthread_mutex_t cache_mutex;

    string* __fetch_item(const string &key)
    {
      pthread_mutex_lock(&cache_mutex);
        Entry entry;
	EntryList::iterator* ptrItr = (EntryList::iterator*) mapOfListIteratorPtr[key];

        if (!ptrItr) /* Key Not Found */
        {
	  return NULL;
        } else {
	  /* Key Found */
 	  entry = *(*ptrItr);
/* 	  listOfEntries.erase(*ptrItr); */
/* 	  listOfEntries.push_front(entry); */
/* 	  *ptrItr = listOfEntries.begin(); */
        }
	string* retString = new string (entry.value->c_str());

	pthread_mutex_unlock(&cache_mutex);
        return retString;
    }


    void __evict_item(const string &key) {
      pthread_mutex_lock(&cache_mutex);
        Entry entry;
	EntryList::iterator* ptrItr = (EntryList::iterator*) mapOfListIteratorPtr[key];

	if (ptrItr) { /* Key Found */
	  entry = *(*ptrItr);
	  HandleItemRelease(entry.key, entry.value);
	  listOfEntries.erase(*ptrItr);
	  delete  (EntryList::iterator*)mapOfListIteratorPtr[key];
	  mapOfListIteratorPtr.erase(key);
	} 
      pthread_mutex_unlock(&cache_mutex);
    }


    void  __add_item(const string &key, string* value) {
      pthread_mutex_lock(&cache_mutex);
	EntryList::iterator* ptrItr = (EntryList::iterator*) mapOfListIteratorPtr[key];

	//cout << "Adding key. Num entries: " <<  (int)listOfEntries.size() << endl;

        if (!ptrItr) /* Key Not Found */
        {
            if ( (int)listOfEntries.size() >= maxLength)
            {
	      /* Evict Entry */
                Entry lruEntry = listOfEntries.back();
                listOfEntries.pop_back();
                delete  (EntryList::iterator*)mapOfListIteratorPtr[*lruEntry.key];
		mapOfListIteratorPtr.erase(*lruEntry.key);
		HandleItemRelease(lruEntry.key, lruEntry.value);
            }
        } else {

	  /* Key found, evict old entry */
	  //__evict_item(key);
	  pthread_mutex_unlock(&cache_mutex);
	  return;
	}

	/* Create New Entry */
	Entry entry;

	string* val = new string(value->c_str());
	entry.value = val;
	entry.key = new string(key);

	listOfEntries.push_front(entry);
	
	EntryList::iterator* ptrItrtemp = new EntryList::iterator();
	*ptrItrtemp = listOfEntries.begin();
	mapOfListIteratorPtr[key] = ptrItrtemp;
	pthread_mutex_unlock(&cache_mutex);
    }


    virtual void __clear()
    {
#ifdef DEBUG
      cout << "Clearing" << endl;
#endif
        for (ItrPtrMap::iterator i=mapOfListIteratorPtr.begin(); i!=mapOfListIteratorPtr.end(); i++)
        {
            void* ptrItr = i->second;

            EntryList::iterator* pItr = (EntryList::iterator*) ptrItr;


	    if (pItr != NULL) { /* Check for null iterator before dereferencing */
	      HandleItemRelease( (*pItr)->key, (*pItr)->value );
	    }

            delete  (EntryList::iterator*)ptrItr;
        }

        listOfEntries.clear();
        mapOfListIteratorPtr.clear();
    }
};


class FECache : public MruCache
{
public:
    FECache(int iMaxLength) : MruCache(iMaxLength) { }

protected:
    virtual void HandleItemRelease(string* k, string* v)
    {
      if (k) {
	delete k;
      }

      if (v) {
	delete v;
      }
    }

    virtual string* HandleNonExistingKeyFetch(string* k)
    {
#ifdef DEBUG
      cout << "[DEBUG] key " << *k << " not found" << endl;
#endif
	return NULL;
    }

};






