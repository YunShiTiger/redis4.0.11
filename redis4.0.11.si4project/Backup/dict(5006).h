/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 */

#include <stdint.h>

#ifndef __DICT_H
#define __DICT_H

#define DICT_OK 0
#define DICT_ERR 1

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void) V)

/* hash表结构中的节点元素结构 */
typedef struct dictEntry {
    //key部分
    void *key;
	//value部分
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
	//指向下一个hash节点，用来解决hash键冲突（collision）
    struct dictEntry *next;
} dictEntry;

/*dictType类型保存着 操作字典不同类型key和value的方法 的指针*/
typedef struct dictType {
	//计算hash值的函数
    uint64_t (*hashFunction)(const void *key);
	//复制key的函数
    void *(*keyDup)(void *privdata, const void *key);
	//复制value的函数
    void *(*valDup)(void *privdata, const void *obj);
	//比较key的函数
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);
	//销毁key的析构函数
    void (*keyDestructor)(void *privdata, void *key);
	//销毁val的析构函数
    void (*valDestructor)(void *privdata, void *obj);
} dictType;

/* This is our hash table structure. Every dictionary has two of this as we implement incremental rehashing, for the old to the new table. */
/* redis中实现的hash表结构---->哈希表 */
typedef struct dictht {
    //存放一个数组的地址，数组存放着哈希表节点dictEntry的地址
    dictEntry **table;
	//哈希表table的大小，初始化大小为4
    unsigned long size;
	//用于将哈希值映射到table的位置索引。它的值总是等于(size-1)
    unsigned long sizemask;
	//记录哈希表已有的节点（键值对）数量
    unsigned long used;
} dictht;

/*redis中实现的字典结构*/
typedef struct dict {
    //指向dictType结构，dictType结构中包含自定义的函数，这些函数使得key和value能够存储任何类型的数据
    dictType *type;
	//私有数据，保存着dictType结构中函数的参数
    void *privdata;
	//两张哈希表
    dictht ht[2];
	/* rehashing not in progress if rehashidx == -1 */
	//rehash的标记，rehashidx==-1，表示没在进行rehash
    long rehashidx; 
	/* number of iterators currently running */
	//正在迭代的迭代器数量
    unsigned long iterators; 
} dict;

/* 
 * If safe is set to 1 this is a safe iterator, that means, you can call
 * dictAdd, dictFind, and other functions against the dictionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictNext() should be called while iterating. 
 */
typedef struct dictIterator {
    dict *d;
    long index;
    int table, safe;
    dictEntry *entry, *nextEntry;
    /* unsafe iterator fingerprint for misuse detection. */
    long long fingerprint;
} dictIterator;

typedef void (dictScanFunction)(void *privdata, const dictEntry *de);
typedef void (dictScanBucketFunction)(void *privdata, dictEntry **bucketref);

/* This is the initial size of every hash table */
#define DICT_HT_INITIAL_SIZE     4

/* ------------------------------- Macros ------------------------------------*/
#define dictFreeVal(d, entry)                                         \
    if ((d)->type->valDestructor)                                     \
        (d)->type->valDestructor((d)->privdata, (entry)->v.val)

#define dictSetVal(d, entry, _val_) do {                              \
    if ((d)->type->valDup)                                            \
        (entry)->v.val = (d)->type->valDup((d)->privdata, _val_);     \
    else                                                              \
        (entry)->v.val = (_val_);                                     \
} while(0)

#define dictSetSignedIntegerVal(entry, _val_)                         \
    do { (entry)->v.s64 = _val_; } while(0)

#define dictSetUnsignedIntegerVal(entry, _val_)                       \
    do { (entry)->v.u64 = _val_; } while(0)

#define dictSetDoubleVal(entry, _val_)                                \
    do { (entry)->v.d = _val_; } while(0)

#define dictFreeKey(d, entry)                                         \
    if ((d)->type->keyDestructor)                                     \
        (d)->type->keyDestructor((d)->privdata, (entry)->key)

#define dictSetKey(d, entry, _key_) do {                              \
    if ((d)->type->keyDup)                                            \
        (entry)->key = (d)->type->keyDup((d)->privdata, _key_);       \
    else                                                              \
        (entry)->key = (_key_);                                       \
} while(0)

#define dictCompareKeys(d, key1, key2)                                \
    (((d)->type->keyCompare) ?                                        \
        (d)->type->keyCompare((d)->privdata, key1, key2) :            \
        (key1) == (key2))

#define dictHashKey(d, key) (d)->type->hashFunction(key)
#define dictGetKey(he) ((he)->key)
#define dictGetVal(he) ((he)->v.val)
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetDoubleVal(he) ((he)->v.d)
#define dictSlots(d) ((d)->ht[0].size+(d)->ht[1].size)
#define dictSize(d) ((d)->ht[0].used+(d)->ht[1].used)
#define dictIsRehashing(d) ((d)->rehashidx != -1)

/* API */
dict *dictCreate(dictType *type, void *privDataPtr);
int dictExpand(dict *d, unsigned long size);
int dictAdd(dict *d, void *key, void *val);
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing);
dictEntry *dictAddOrFind(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
int dictDelete(dict *d, const void *key);
dictEntry *dictUnlink(dict *ht, const void *key);
void dictFreeUnlinkedEntry(dict *d, dictEntry *he);
void dictRelease(dict *d);
dictEntry * dictFind(dict *d, const void *key);
void *dictFetchValue(dict *d, const void *key);
int dictResize(dict *d);
dictIterator *dictGetIterator(dict *d);
dictIterator *dictGetSafeIterator(dict *d);
dictEntry *dictNext(dictIterator *iter);
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *d);
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count);
void dictGetStats(char *buf, size_t bufsize, dict *d);
uint64_t dictGenHashFunction(const void *key, int len);
uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len);
void dictEmpty(dict *d, void(callback)(void*));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(uint8_t *seed);
uint8_t *dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, dictScanBucketFunction *bucketfn, void *privdata);
uint64_t dictGetHash(dict *d, const void *key);
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash);

/* Hash table types */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#endif /* __DICT_H */













