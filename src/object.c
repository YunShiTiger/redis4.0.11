/* 
 * Redis Object implementation.
 */

#include "server.h"
#include <math.h>
#include <ctype.h>

#ifdef __CYGWIN__
//将a和b转换为long double类型
#define strtold(a,b) ((long double)strtod((a),(b)))
#endif

/* ===================== Creation and parsing of objects ==================== */

//创建一个默认的对象
robj *createObject(int type, void *ptr) {
    //分配空间
    robj *o = zmalloc(sizeof(*o));
	//设置对象类型
    o->type = type;
	//设置默认的编码方式
    o->encoding = OBJ_ENCODING_RAW;
	//设置数据指向
    o->ptr = ptr;
	//引用计数为1
    o->refcount = 1;

    //计算设置当前LRU时间
    /* Set the LRU to the current lruclock (minutes resolution), or
     * alternatively the LFU counter. */
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        o->lru = (LFUGetTimeInMinutes()<<8) | LFU_INIT_VAL;
    } else {
        o->lru = LRU_CLOCK();
    }
	//返回创建的对象
    return o;
}

/* 将一个普通类型的对象设置为共享类型对象----->即改变对象的引用计数值为一个特定的共享类型值
 * Set a special refcount in the object to make it "shared":
 * incrRefCount and decrRefCount() will test for this special refcount
 * and will not touch the object. This way it is free to access shared
 * objects such as small integers from different threads without any
 * mutex.
 *
 * A common patter to create shared objects:
 *
 * robj *myobject = makeObjectShared(createObject(...));
 *
 */
robj *makeObjectShared(robj *o) {
    serverAssert(o->refcount == 1);
	//设置共享类型的引用计数值
    o->refcount = OBJ_SHARED_REFCOUNT;
    return o;
}

/* 创建一个字符串对象，编码默认为OBJ_ENCODING_RAW，指向的数据为一个sds
 * Create a string object with encoding OBJ_ENCODING_RAW, that is a plain
 * string object where o->ptr points to a proper sds string. 
 */
robj *createRawStringObject(const char *ptr, size_t len) {
    return createObject(OBJ_STRING, sdsnewlen(ptr,len));
}

/* 创建一个embstr编码的字符串对象
 * Create a string object with encoding OBJ_ENCODING_EMBSTR, that is
 * an object where the sds string is actually an unmodifiable string
 * allocated in the same chunk as the object itself. 
 */
robj *createEmbeddedStringObject(const char *ptr, size_t len) {
	//分配对应大小的空间
    robj *o = zmalloc(sizeof(robj)+sizeof(struct sdshdr8)+len+1);
	//o+1刚好就是struct sdshdr8的地址
    struct sdshdr8 *sh = (void*)(o+1);

	//设置对象类型为字符串对象
    o->type = OBJ_STRING;
	//设置编码类型OBJ_ENCODING_EMBSTR
    o->encoding = OBJ_ENCODING_EMBSTR;
	//指向分配的sds对象，分配的len+1的空间首地址
    o->ptr = sh+1;
	//设置引用计数
    o->refcount = 1;
	//计算设置当前LRU时间
    if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        o->lru = (LFUGetTimeInMinutes()<<8) | LFU_INIT_VAL;
    } else {
        o->lru = LRU_CLOCK();
    }

	//设置字符串长度
    sh->len = len;
	//设置最大容量
    sh->alloc = len;
	//设置sds的类型
    sh->flags = SDS_TYPE_8;
	//如果传了字符串参数
    if (ptr) {
		//将传进来的ptr保存到对象中
        memcpy(sh->buf,ptr,len);
		//结束符标志
        sh->buf[len] = '\0';
    } else {
		//否则将对象的空间初始化为0
        memset(sh->buf,0,len+1);
    }
	//返回对应的对象
    return o;
}

/* Create a string object with EMBSTR encoding if it is smaller than
 * OBJ_ENCODING_EMBSTR_SIZE_LIMIT, otherwise the RAW encoding is
 * used.
 *
 * The current limit of 39 is chosen so that the biggest string object
 * we allocate as EMBSTR will still fit into the 64 byte arena of jemalloc. 
 */
//sdshdr8的大小为3个字节，加上1个结束符共4个字节
//redisObject的大小为16个字节
//redis使用jemalloc内存分配器，且jemalloc会分配8，16，32，64等字节的内存
//一个embstr固定的大小为16+3+1 = 20个字节，因此一个最大的embstr字符串为64-20 = 44字节
#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 44

// 创建字符串对象，根据长度使用不同的编码类型
// createRawStringObject和createEmbeddedStringObject的区别是：
//      createRawStringObject是当字符串长度大于44字节时，robj结构和sdshdr结构在内存上是分开的
//      createEmbeddedStringObject是当字符串长度小于等于44字节时，robj结构和sdshdr结构在内存上是连续的
robj *createStringObject(const char *ptr, size_t len) {
    //根据字符串长度来确定对应的创建方式
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT)
        return createEmbeddedStringObject(ptr,len);
    else
        return createRawStringObject(ptr,len);
}

//根据整数值来创建字符串对象
robj *createStringObjectFromLongLong(long long value) {
    robj *o;
	//redis中[0, 10000)内的整数是共享的
	//如果value属于redis共享整数的范围
    if (value >= 0 && value < OBJ_SHARED_INTEGERS) {
		//引用计数加1
        incrRefCount(shared.integers[value]);
		//返回一个编码类型为OBJ_ENCODING_INT的字符串对象
        o = shared.integers[value];
    } else {
		//value在long类型所表示的范围内
        if (value >= LONG_MIN && value <= LONG_MAX) {
			//创建对象
            o = createObject(OBJ_STRING, NULL);
			//编码类型为OBJ_ENCODING_INT
            o->encoding = OBJ_ENCODING_INT;
		    //指向这个value值
            o->ptr = (void*)((long)value);
        } else {
			//value不在long类型所表示的范围内，将long long类型的整数转换为字符串
			//编码类型为OBJ_ENCODING_RAW
            o = createObject(OBJ_STRING,sdsfromlonglong(value));
        }
    }
	//返回创建的字符串类型对象
    return o;
}

/* 将long double类型的value创建一个字符串对象
 * Create a string object from a long double. If humanfriendly is non-zero
 * it does not use exponential format and trims trailing zeroes at the end,
 * however this results in loss of precision. Otherwise exp format is used
 * and the output of snprintf() is not modified.
 *
 * The 'humanfriendly' option is used for INCRBYFLOAT and HINCRBYFLOAT. 
 */
robj *createStringObjectFromLongDouble(long double value, int humanfriendly) {
    //创建对应的存储转换数据之后的字符串
    char buf[MAX_LONG_DOUBLE_CHARS];
	//进行转换操作处理
    int len = ld2string(buf,sizeof(buf),value,humanfriendly);
	//创建对应的字符串对象
    return createStringObject(buf,len);
}

/* 返回 复制的o对象的副本的地址，且创建的对象非共享
 * Duplicate a string object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * This function also guarantees that duplicating a small integere object
 * (or a string object that contains a representation of a small integer)
 * will always result in a fresh object that is unshared (refcount == 1).
 *
 * The resulting object always has refcount set to 1. 
 */
robj *dupStringObject(const robj *o) {
    robj *d;
    //一定是OBJ_STRING类型
    serverAssert(o->type == OBJ_STRING);
	//根据编码类型进行不同的复制操作处理
    switch(o->encoding) {
    	case OBJ_ENCODING_RAW:
			//创建的对象非共享
        	return createRawStringObject(o->ptr,sdslen(o->ptr));
    	case OBJ_ENCODING_EMBSTR:
			//创建的对象非共享
        	return createEmbeddedStringObject(o->ptr,sdslen(o->ptr));
    	case OBJ_ENCODING_INT:
			//即使是共享整数范围内的整数，创建的对象也是非共享的
        	d = createObject(OBJ_STRING, NULL);
        	d->encoding = OBJ_ENCODING_INT;
        	d->ptr = o->ptr;
        	return d;
    	default:
        	serverPanic("Wrong encoding.");
        	break;
    }
}

//创建一个quicklist编码的列表对象
robj *createQuicklistObject(void) {
    //创建一个quicklist
    quicklist *l = quicklistCreate();
	//创建一个对象，对象的数据类型为OBJ_LIST
    robj *o = createObject(OBJ_LIST,l);
	//对象的编码类型OBJ_ENCODING_QUICKLIST
    o->encoding = OBJ_ENCODING_QUICKLIST;
	//返回对应的对象
    return o;
}

//创建一个ziplist编码的列表对象
robj *createZiplistObject(void) {
    //创建一个ziplist
    unsigned char *zl = ziplistNew();
	//创建一个对象，对象的数据类型为OBJ_LIST
    robj *o = createObject(OBJ_LIST,zl);
	//对象的编码类型OBJ_ENCODING_ZIPLIST
    o->encoding = OBJ_ENCODING_ZIPLIST;
	//返回对应的对象
    return o;
}

/* 创建对应的字典类型集合对象 */
robj *createSetObject(void) {
    //创建对应的字典结构
    dict *d = dictCreate(&setDictType,NULL);
	//创建对应的字典结构对象
    robj *o = createObject(OBJ_SET,d);
	//设置对象的编码实现方式
    o->encoding = OBJ_ENCODING_HT;
	//返回对应的对象
    return o;
}

/* 创建对应的整数集合类型对象*/
robj *createIntsetObject(void) {
    //创建对应的整数集合结构
    intset *is = intsetNew();
	//创建对应的整数集合对象
    robj *o = createObject(OBJ_SET,is);
	//设置对象的编码实现方式
    o->encoding = OBJ_ENCODING_INTSET;
	//返回对应的对象
    return o;
}

//创建一个ziplist编码的哈希对象
robj *createHashObject(void) {
    //创建一个ziplist
    unsigned char *zl = ziplistNew();
	//创建一个对象，对象的数据类型为OBJ_HASH
    robj *o = createObject(OBJ_HASH, zl);
	//对象的编码类型OBJ_ENCODING_ZIPLIST
    o->encoding = OBJ_ENCODING_ZIPLIST;
	//返回对应的对象
    return o;
}

//创建一个skiplist编码的有序集合对象
robj *createZsetObject(void) {
    //创建对应的zset结构
    zset *zs = zmalloc(sizeof(*zs));
    robj *o;

    //创建一个字典
    zs->dict = dictCreate(&zsetDictType,NULL);
	//创建一个跳跃表
    zs->zsl = zslCreate();
	//创建一个对象，对象的数据类型为OBJ_ZSET
    o = createObject(OBJ_ZSET,zs);
	//对象的编码类型OBJ_ENCODING_SKIPLIST
    o->encoding = OBJ_ENCODING_SKIPLIST;
	//返回对应的对象
    return o;
}

//创建一个ziplist编码的有序集合对象
robj *createZsetZiplistObject(void) {
    //创建一个ziplist
    unsigned char *zl = ziplistNew();
	//创建一个对象，对象的数据类型为OBJ_ZSET
    robj *o = createObject(OBJ_ZSET,zl);
	//对象的编码类型OBJ_ENCODING_ZIPLIST
    o->encoding = OBJ_ENCODING_ZIPLIST;
	//返回对应的对象
    return o;
}

//创建对应的模块对象类型
robj *createModuleObject(moduleType *mt, void *value) {
    //首先分配对应的空间
    moduleValue *mv = zmalloc(sizeof(*mv));
	//设置模块类型
    mv->type = mt;
	//设置对应的值
    mv->value = value;
	//创建对应的模块对象
    return createObject(OBJ_MODULE,mv);
}

//释放字符串对象ptr指向的对象
void freeStringObject(robj *o) {
    //检测字符串对象的编码方式-------->即对象和数据是否分离的
    if (o->encoding == OBJ_ENCODING_RAW) {
		//释放对应的数据部分的空间
        sdsfree(o->ptr);
    }
}

//释放列表对象ptr指向的对象
void freeListObject(robj *o) {
    //检测List列表对象的编码方式
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
		//释放对应的数据部分空间
        quicklistRelease(o->ptr);
    } else {
        serverPanic("Unknown list encoding type");
    }
}

//释放集合对象ptr指向的对象
void freeSetObject(robj *o) {
    //检测集合的编码方式
    switch (o->encoding) {
    	case OBJ_ENCODING_HT:
			//释放对应的数据部分空间
        	dictRelease((dict*) o->ptr);
        	break;
    	case OBJ_ENCODING_INTSET:
			//释放对应的数据部分空间
        	zfree(o->ptr);
        	break;
    	default:
        	serverPanic("Unknown set encoding type");
    }
}

//释放有序集合对象ptr指向的对象
void freeZsetObject(robj *o) {
    zset *zs;
	//检测有序集合的编码方式
    switch (o->encoding) {
    	case OBJ_ENCODING_SKIPLIST:
			//释放对应的数据部分空间
        	zs = o->ptr;
        	dictRelease(zs->dict);
        	zslFree(zs->zsl);
        	zfree(zs);
        	break;
   	 	case OBJ_ENCODING_ZIPLIST:
			//释放对应的数据部分空间
        	zfree(o->ptr);
        	break;
    	default:
        	serverPanic("Unknown sorted set encoding");
    }
}

//释放哈希对象ptr指向的对象
void freeHashObject(robj *o) {
    //检测哈希对象的编码方式
    switch (o->encoding) {
    	case OBJ_ENCODING_HT:
			//释放对应的数据部分空间
        	dictRelease((dict*) o->ptr);
        	break;
    	case OBJ_ENCODING_ZIPLIST:
			//释放对应的数据部分空间
        	zfree(o->ptr);
       	 	break;
    	default:
        	serverPanic("Unknown hash encoding type");
        	break;
    }
}

//释放模块对象占据的空间
void freeModuleObject(robj *o) {
    //获取对应的数据指向
    moduleValue *mv = o->ptr;
	//释放对应的值
    mv->type->free(mv->value);
	//释放对应的空间
    zfree(mv);
}

/*增加对应值对象的引用计数值*/
void incrRefCount(robj *o) {
    //检测是否是共享类型对象
    if (o->refcount != OBJ_SHARED_REFCOUNT) 
		//增加对应的引用计数值
		o->refcount++;
}

/*减少对应的对象的引用计数---->如果对应的对象引用计数为1,再减少说明没有对象对其进行引用了,需要进行释放对应的对象空间了*/
void decrRefCount(robj *o) {
    //检测是否还有其他的对象对其进行引用---->没有就根据类型进行空间释放操作处理
    if (o->refcount == 1) {
		//根据值对象的类型不同,触发不同的释放空间操作处理
        switch(o->type) {
        	case OBJ_STRING: 
				freeStringObject(o); 
				break;
        	case OBJ_LIST: 
				freeListObject(o); 
				break;
        	case OBJ_SET: 
				freeSetObject(o); 
				break;
        	case OBJ_ZSET: 
				freeZsetObject(o); 
				break;
        	case OBJ_HASH: 
				freeHashObject(o); 
				break;
        	case OBJ_MODULE: 
				freeModuleObject(o); 
				break;
        	default: 
        	serverPanic("Unknown object type"); 
			break;
        }
		//最后将本值对象占据的空间也触发释放空间操作处理
        zfree(o);
    } else {
		//检测引用计数是否减少到了负数的异常处理
        if (o->refcount <= 0) 
			serverPanic("decrRefCount against refcount <= 0");
		//检测对应的对象是否是共享类型对象
        if (o->refcount != OBJ_SHARED_REFCOUNT) 
			o->refcount--;
    }
}

/* 释放特定的数据类型的封装
 * This variant of decrRefCount() gets its argument as void, and is useful
 * as free method in data structures that expect a 'void free_object(void*)' prototype for the free method. 
 */
void decrRefCountVoid(void *o) {
    decrRefCount(o);
}

/* 重置obj对象的引用计数为0，用于上面注释中的情况
 * This function set the ref count to zero without freeing the object.
 * It is useful in order to pass a new object to functions incrementing
 * the ref count of the received object. Example:
 *
 *    functionThatWillIncrementRefCount(resetRefCount(CreateObject(...)));
 *
 * Otherwise you need to resort to the less elegant pattern:
 *
 *    *obj = createObject(...);
 *    functionThatWillIncrementRefCount(obj);
 *    decrRefCount(obj);
 */
robj *resetRefCount(robj *obj) {
    obj->refcount = 0;
    return obj;
}

/* 检测对应的值对象是否是对应类型的值对象 */
int checkType(client *c, robj *o, int type) {
    //检测对应的值对象类型是否与对应的类型相同
    if (o->type != type) {
		//给客户端返回类型不匹配错误
        addReply(c,shared.wrongtypeerr);
		//返回不不匹配标识
        return 1;
    }
	//返回类型匹配标识
    return 0;
}

//判断对应的sds类型是否可以转换成对应的整数值
int isSdsRepresentableAsLongLong(sds s, long long *llval) {
    //进行尝试转换操作处理
    return string2ll(s,sdslen(s),llval) ? C_OK : C_ERR;
}

//判断对象的ptr指向的值能否转换为long long类型，如果可以保存在llval中
int isObjectRepresentableAsLongLong(robj *o, long long *llval) {
    //检测类型是否是对应的字符串类型对象
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
	//检测字符串对象的编码方式是否是整数类型
    if (o->encoding == OBJ_ENCODING_INT) {
        if (llval) 
			//直接获取对应的整数值
			*llval = (long) o->ptr;
		//返回获取成功标识
        return C_OK;
    } else {
		//判断对应的sds类型是否可以转换成对应的整数值
        return isSdsRepresentableAsLongLong(o->ptr,llval);
    }
}

/* 为了进行节省空间的处理,尝试将对应的字符串进行转换处理-------------->进行编码方式的优化操作处理
 *     即 如果是对应的RAW和EMBSTR编码类型的,需要进行转换操作处理
 *         --->尝试将其转换成对应的整数类型
 *         --->尝试将短长度使用emb编码类型进行存储
 * Try to encode a string object in order to save space 
 */
robj *tryObjectEncoding(robj *o) {
    long value;
    sds s = o->ptr;
    size_t len;

    /* Make sure this is a string object, the only type we encode
     * in this function. Other types use encoded memory efficient
     * representations but are handled by the commands implementing
     * the type. */
    //确保对应的对象是字符串类型对象
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);

    /* We try some specialized encoding only for objects that are
     * RAW or EMBSTR encoded, in other words objects that are still
     * in represented by an actually array of chars. */
    //如果字符串对象的编码类型为RAW或EMBSTR时，才对其重新编码
    if (!sdsEncodedObject(o)) 
		return o;

    /* It's not safe to encode shared objects: shared objects can be shared
     * everywhere in the "object space" of Redis and may end in places where
     * they are not handled. We handle them only as values in the keyspace. 
     */
     //如果refcount大于1，则说明对象的ptr指向的值是共享的，不对共享对象进行编码
     if (o->refcount > 1) 
	 	return o;

    /* Check if we can represent this string as a long integer.
     * Note that we are sure that a string larger than 20 chars is not
     * representable as a 32 nor 64 bit integer. */
    //获取对应的字符串的长度值
    len = sdslen(s);
	//如果len小于等于20，表示符合long long可以表示的范围，且可以转换为long类型的字符串进行编码
    if (len <= 20 && string2l(s,len,&value)) {
        /* This object is encodable as a long. Try to use a shared object.
         * Note that we avoid using shared integers when maxmemory is used
         * because every object needs to have a private LRU field for the LRU
         * algorithm to work well. */
        //如果value处于共享整数的范围内
        if ((server.maxmemory == 0 ||!(server.maxmemory_policy & MAXMEMORY_FLAG_NO_SHARED_INTEGERS)) &&
            value >= 0 && value < OBJ_SHARED_INTEGERS) {
            //原对象的引用计数减1，释放对象
            decrRefCount(o);
			//增加共享对象的引用计数
            incrRefCount(shared.integers[value]);
			//返回一个编码为整数的字符串对象
            return shared.integers[value];
        } else {
            if (o->encoding == OBJ_ENCODING_RAW) 
				//释放编码为OBJ_ENCODING_RAW的对象
				sdsfree(o->ptr);
			//转换为OBJ_ENCODING_INT编码
            o->encoding = OBJ_ENCODING_INT;
			//指针ptr指向value对象
            o->ptr = (void*) value;
			//返回对应的对象
            return o;
        }
    }

    /* If the string is small and is still RAW encoded,
     * try the EMBSTR encoding which is more efficient.
     * In this representation the object and the SDS string are allocated
     * in the same chunk of memory to save space and cache misses. 
     */
    //如果len小于44，44是最大的编码为EMBSTR类型的字符串对象长度
    if (len <= OBJ_ENCODING_EMBSTR_SIZE_LIMIT) {
        robj *emb;
		//检测当前字符串的编码方式是否已经处于emb类型
        if (o->encoding == OBJ_ENCODING_EMBSTR) 
			return o;
	    //创建一个编码类型为OBJ_ENCODING_EMBSTR的字符串对象
        emb = createEmbeddedStringObject(s,sdslen(s));
		//释放之前的对象
        decrRefCount(o);
        return emb;
    }

    /* We can't encode the object...
     *
     * Do the last try, and at least optimize the SDS string inside
     * the string object to require little space, in case there
     * is more than 10% of free space at the end of the SDS string.
     *
     * We do that only for relatively large strings as this branch
     * is only entered if the length of the string is greater than
     * OBJ_ENCODING_EMBSTR_SIZE_LIMIT. */
    if (o->encoding == OBJ_ENCODING_RAW && sdsavail(s) > len/10) {
        o->ptr = sdsRemoveFreeSpace(o->ptr);
    }

    /* Return the original object. */
    return o;
}

/* 获取给定字符串类型对象的解码字符串对象------->即返回的总是对应的字符串形式的数据(即整数类型的需要转换成对应的字符串形式)
 * 将对象是整型的解码为字符串并返回，如果是字符串编码则直接返回输入对象(只需增加引用计数)
 * Get a decoded version of an encoded object (returned as a new object).
 * If the object is already raw-encoded just increment the ref count.
 */
robj *getDecodedObject(robj *o) {
    robj *dec;
	//检测是否是字符串类型的对象
    if (sdsEncodedObject(o)) {
		//增加本字符串类型的引用计数
        incrRefCount(o);
		//返回增加引用计数后的对象
        return o;
    }
	//处理对象是字符串类型对象,同时实现方式是编码整数类型
    if (o->type == OBJ_STRING && o->encoding == OBJ_ENCODING_INT) {
        char buf[32];
		//获取整数所对应的字符串类型数据
        ll2string(buf,32,(long)o->ptr);
	    //创建对应的字符串类型数据
        dec = createStringObject(buf,strlen(buf));
		//返回新创建的字符串类型对象
        return dec;
    } else {
        serverPanic("Unknown encoding type");
    }
}

/* Compare two string objects via strcmp() or strcoll() depending on flags.
 * Note that the objects may be integer-encoded. In such a case we
 * use ll2string() to get a string representation of the numbers on the stack
 * and compare the strings, it's much faster than calling getDecodedObject().
 *
 * Important note: when REDIS_COMPARE_BINARY is used a binary-safe comparison is used. 
 */
 
//以二进制方式进行比较标记
#define REDIS_COMPARE_BINARY (1<<0) 
//以本地指定的字符次序进行比较标记
#define REDIS_COMPARE_COLL (1<<1)

//根据flags比较两个字符串对象a和b，返回0表示相等，非零表示不相等
int compareStringObjectsWithFlags(robj *a, robj *b, int flags) {
    serverAssertWithInfo(NULL,a,a->type == OBJ_STRING && b->type == OBJ_STRING);
    char bufa[128], bufb[128], *astr, *bstr;
    size_t alen, blen, minlen;

    //如果是同一对象直接返回
    if (a == b) 
		return 0;

	//处理a对象数据
    if (sdsEncodedObject(a)) {
		//如果是指向字符串值的两种OBJ_ENCODING_EMBSTR或OBJ_ENCODING_RAW的两类对象
        astr = a->ptr;
		//获取字符串的长度
        alen = sdslen(astr);
    } else {
		//如果是整数类型的OBJ_ENCODING_INT编码 转换为字符串
        alen = ll2string(bufa,sizeof(bufa),(long) a->ptr);
        astr = bufa;
    }

	//处理b对象数据
    if (sdsEncodedObject(b)) {
        bstr = b->ptr;
        blen = sdslen(bstr);
    } else {
        blen = ll2string(bufb,sizeof(bufb),(long) b->ptr);
        bstr = bufb;
    }

	
    if (flags & REDIS_COMPARE_COLL) {
		//以本地指定的字符次序进行比较
		//strcoll()会依环境变量LC_COLLATE所指定的文字排列次序来比较两字符串
        return strcoll(astr,bstr);
    } else {
        //以二进制方式进行比较
        int cmp;

        minlen = (alen < blen) ? alen : blen;
		//相等返回0，否则返回第一个字符串和第二个字符串的长度差
        cmp = memcmp(astr,bstr,minlen);
        if (cmp == 0) 
			return alen-blen;
        return cmp;
    }
}

/* Wrapper for compareStringObjectsWithFlags() using binary comparison. */
//以二进制安全的方式进行对象的比较
int compareStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a,b,REDIS_COMPARE_BINARY);
}

/* Wrapper for compareStringObjectsWithFlags() using collation. */
//以指定的字符次序进行两个对象的比较
int collateStringObjects(robj *a, robj *b) {
    return compareStringObjectsWithFlags(a,b,REDIS_COMPARE_COLL);
}

/* 比较两个字符串对象，以二进制安全的方式进行比较
 * Equal string objects return 1 if the two objects are the same from the
 * point of view of a string comparison, otherwise 0 is returned. Note that
 * this function is faster then checking for (compareStringObject(a,b) == 0)
 * because it can perform some more optimization. 
 */
int equalStringObjects(robj *a, robj *b) {
    //如果两个对象的编码都是整型INT
    if (a->encoding == OBJ_ENCODING_INT && b->encoding == OBJ_ENCODING_INT){
        /* If both strings are integer encoded just check if the stored long is the same. */
	    //直接比较两个整数值
        return a->ptr == b->ptr;
    } else {
        //否则进行二进制安全比较字符串
        return compareStringObjects(a,b) == 0;
    }
}

//返回字符串对象的字符串长度
size_t stringObjectLen(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
	//如果是字符串编码的两种类型
    if (sdsEncodedObject(o)) {
        return sdslen(o->ptr);
    } else {
        //计算出整数值的位数返回
        return sdigits10((long)o->ptr);
    }
}

//从对象中将字符串值转换为double并存储在target中
int getDoubleFromObject(const robj *o, double *target) {
    double value;
    char *eptr;
	
	//对象不存在
    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
		//如果是字符串编码的两种类型
        if (sdsEncodedObject(o)) {
            errno = 0;
			//将字符串转换为double类型
            value = strtod(o->ptr, &eptr);
            if (sdslen(o->ptr) == 0 || isspace(((const char*)o->ptr)[0]) || (size_t)(eptr-(char*)o->ptr) != sdslen(o->ptr) ||
                (errno == ERANGE && (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) || isnan(value))
                return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
			//保存整数值
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
	//将值存到传入参数中
    *target = value;
	//返回0成功
    return C_OK;
}

//从对象中将字符串值转换为double并存储在target中，若失败，失败发送信息给client
int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg) {
    double value;
	//进行尝试转换操作处理
    if (getDoubleFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
			//msg不为空,发送指定的msg给client
            addReplyError(c,(char*)msg);
        } else {
            //发送默认的错误信息
            addReplyError(c,"value is not a valid float");
        }
		//返回对应的错误标识
        return C_ERR;
    }
	//将转换成功的值存到传入参数中
    *target = value;
	//返回0成功
    return C_OK;
}

//从对象中将字符串值转换为long double并存储在target中
int getLongDoubleFromObject(robj *o, long double *target) {
    long double value;
    char *eptr;
	
	//检测给定的对象是否存在
    if (o == NULL) {
        value = 0;
    } else {
        serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
		//如果是字符串编码的两种类型
        if (sdsEncodedObject(o)) {
            errno = 0;
			//将字符串转换为long double类型
            value = strtold(o->ptr, &eptr);
            if (sdslen(o->ptr) == 0 || isspace(((const char*)o->ptr)[0]) || (size_t)(eptr-(char*)o->ptr) != sdslen(o->ptr) ||
                (errno == ERANGE && (value == HUGE_VAL || value == -HUGE_VAL || value == 0)) || isnan(value))
                return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
			//整数编码,保存整数值
            value = (long)o->ptr;
        } else {
            serverPanic("Unknown string encoding");
        }
    }
	//将值存到传入参数中
    *target = value;
	//返回0成功
    return C_OK;
}

//从对象中将字符串值转换为long double并存储在target中，若失败，失败发送信息给client
int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg) {
    long double value;
	//进行尝试转换操作处理
    if (getLongDoubleFromObject(o, &value) != C_OK) {
        if (msg != NULL) {
			//msg不为空,发送指定的msg给client
            addReplyError(c,(char*)msg);
        } else {
            //发送默认的错误信息
            addReplyError(c,"value is not a valid float");
        }
		//返回对应的错误标识
        return C_ERR;
    }
	//将转换成功的值存到传入参数中
    *target = value;
	//返回0成功
    return C_OK;
}

/*本函数是获取对象中存储的整数数据的核心处理函数------>即真正完成从对象中获取记录的整数数据的处理*/
int getLongLongFromObject(robj *o, long long *target) {
    long long value;
	//检测给定的对象是否为空
    if (o == NULL) {
		//直接设置0值
        value = 0;
    } else {
        serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
		//检测对应的数据是否是字符串类型的数据对象
        if (sdsEncodedObject(o)) {
			//尝试将对应的字符串类型数据转化成对应的整数类型数据--------->这个地方是尝试进行转换操作处理------>本函数的核心操作
            if (string2ll(o->ptr,sdslen(o->ptr),&value) == 0) 
				return C_ERR;
        } else if (o->encoding == OBJ_ENCODING_INT) {
            //直接获取存储的整数数据
            value = (long)o->ptr;
        } else {
			//其他类型的数据对象错误
            serverPanic("Unknown string encoding");
        }
    }

	//检测是否配置了对应的存储数据的空间
    if (target) 
		//设置获取到的整数数据
		*target = value;
	
	//返回获取整数数据成功的标识
    return C_OK;
}

/*本函数在获取对象中整数数据函数的基础上添加了数据获取失败直接向客户端进行响应的处理------>即在整数失败时向客户端进行发送失败原因*/
int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg) {
    long long value;
	//调用处理获取对象中整数数据的核心处理函数进行获取整数数据是否成功
    if (getLongLongFromObject(o, &value) != C_OK) {
		//检测是否配置了失败消息
        if (msg != NULL) {
            addReplyError(c,(char*)msg);
        } else {
            addReplyError(c,"value is not an integer or out of range");
        }
		//返回获取对应的整数数据失败的错误标识
        return C_ERR;
    }
	//记录获取到的整数数据
    *target = value;
	//返回获取数据成功的标识
    return C_OK;
}

/*从对应的对象中获取一个整数数据,如果获取失败直接向对应的客户端响应获取数据失败,已经失败的原因*/
int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg) {
    //定义变量,用于存储对应的整数值
    long long value;

    //获取对应对象的整数值------>此处调用的函数具有获取数据失败直接进行相应操作处理的功能
    if (getLongLongFromObjectOrReply(c, o, &value, msg) != C_OK) 
		return C_ERR;
	
	//检测获取的整数值是否超过的对应的范围--------------->本函数在核心函数的功能上添加了对获取的整数进行范围是否越界的判断处理
    if (value < LONG_MIN || value > LONG_MAX) {
		//检测是否配置了需要发送失败的消息
        if (msg != NULL) {
			//向客户端发送获取对应参数数据失败的原因
            addReplyError(c,(char*)msg);
        } else {
            //向客户端发送获取整数数据失败的原因是给定的整数范围超出了范围
            addReplyError(c,"value is out of range");
        }
		//返回错误标识
        return C_ERR;
    }
	//记录获取到的整数数据
    *target = value;
	//返回获取对应整数数据正确的标识
    return C_OK;
}

//返回编码类型的字符串表示
char *strEncoding(int encoding) {
    switch(encoding) {
    	case OBJ_ENCODING_RAW: 
			return "raw";
    	case OBJ_ENCODING_INT: 
			return "int";
    	case OBJ_ENCODING_HT: 
			return "hashtable";
    	case OBJ_ENCODING_QUICKLIST: 
			return "quicklist";
    	case OBJ_ENCODING_ZIPLIST: 
			return "ziplist";
    	case OBJ_ENCODING_INTSET: 
			return "intset";
    	case OBJ_ENCODING_SKIPLIST: 
			return "skiplist";
    	case OBJ_ENCODING_EMBSTR: 
			return "embstr";
    	default: return "unknown";
    }
}

/* =========================== Memory introspection ========================== */

/* Returns the size in bytes consumed by the key's value in RAM.
 * Note that the returned value is just an approximation, especially in the
 * case of aggregated data types where only "sample_size" elements
 * are checked and averaged to estimate the total size. 
 */
#define OBJ_COMPUTE_SIZE_DEF_SAMPLES 5 /* Default sample size. */
size_t objectComputeSize(robj *o, size_t sample_size) {
    sds ele, ele2;
    dict *d;
    dictIterator *di;
    struct dictEntry *de;
    size_t asize = 0, elesize = 0, samples = 0;

    if (o->type == OBJ_STRING) {
        if(o->encoding == OBJ_ENCODING_INT) {
            asize = sizeof(*o);
        } else if(o->encoding == OBJ_ENCODING_RAW) {
            asize = sdsAllocSize(o->ptr)+sizeof(*o);
        } else if(o->encoding == OBJ_ENCODING_EMBSTR) {
            asize = sdslen(o->ptr)+2+sizeof(*o);
        } else {
            serverPanic("Unknown string encoding");
        }
    } else if (o->type == OBJ_LIST) {
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;
            asize = sizeof(*o)+sizeof(quicklist);
            do {
                elesize += sizeof(quicklistNode)+ziplistBlobLen(node->zl);
                samples++;
            } while ((node = node->next) && samples < sample_size);
            asize += (double)elesize/samples*ql->len;
        } else if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            asize = sizeof(*o)+ziplistBlobLen(o->ptr);
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        if (o->encoding == OBJ_ENCODING_HT) {
            d = o->ptr;
            di = dictGetIterator(d);
            asize = sizeof(*o)+sizeof(dict)+(sizeof(struct dictEntry*)*dictSlots(d));
            while((de = dictNext(di)) != NULL && samples < sample_size) {
                ele = dictGetKey(de);
                elesize += sizeof(struct dictEntry) + sdsAllocSize(ele);
                samples++;
            }
            dictReleaseIterator(di);
            if (samples) asize += (double)elesize/samples*dictSize(d);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            intset *is = o->ptr;
            asize = sizeof(*o)+sizeof(*is)+is->encoding*is->length;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            asize = sizeof(*o)+(ziplistBlobLen(o->ptr));
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            d = ((zset*)o->ptr)->dict;
            zskiplist *zsl = ((zset*)o->ptr)->zsl;
            zskiplistNode *znode = zsl->header->level[0].forward;
            asize = sizeof(*o)+sizeof(zset)+(sizeof(struct dictEntry*)*dictSlots(d));
            while(znode != NULL && samples < sample_size) {
                elesize += sdsAllocSize(znode->ele);
                elesize += sizeof(struct dictEntry) + zmalloc_size(znode);
                samples++;
                znode = znode->level[0].forward;
            }
            if (samples) asize += (double)elesize/samples*dictSize(d);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            asize = sizeof(*o)+(ziplistBlobLen(o->ptr));
        } else if (o->encoding == OBJ_ENCODING_HT) {
            d = o->ptr;
            di = dictGetIterator(d);
            asize = sizeof(*o)+sizeof(dict)+(sizeof(struct dictEntry*)*dictSlots(d));
            while((de = dictNext(di)) != NULL && samples < sample_size) {
                ele = dictGetKey(de);
                ele2 = dictGetVal(de);
                elesize += sdsAllocSize(ele) + sdsAllocSize(ele2);
                elesize += sizeof(struct dictEntry);
                samples++;
            }
            dictReleaseIterator(di);
            if (samples) asize += (double)elesize/samples*dictSize(d);
        } else {
            serverPanic("Unknown hash encoding");
        }
    } else if (o->type == OBJ_MODULE) {
        moduleValue *mv = o->ptr;
        moduleType *mt = mv->type;
        if (mt->mem_usage != NULL) {
            asize = mt->mem_usage(mv->value);
        } else {
            asize = 0;
        }
    } else {
        serverPanic("Unknown object type");
    }
    return asize;
}

/* Release data obtained with getMemoryOverheadData(). */
void freeMemoryOverheadData(struct redisMemOverhead *mh) {
    zfree(mh->db);
    zfree(mh);
}

/* Return a struct redisMemOverhead filled with memory overhead
 * information used for the MEMORY OVERHEAD and INFO command. The returned
 * structure pointer should be freed calling freeMemoryOverheadData(). */
struct redisMemOverhead *getMemoryOverheadData(void) {
    int j;
    size_t mem_total = 0;
    size_t mem = 0;
    size_t zmalloc_used = zmalloc_used_memory();
    struct redisMemOverhead *mh = zcalloc(sizeof(*mh));

    mh->total_allocated = zmalloc_used;
    mh->startup_allocated = server.initial_memory_usage;
    mh->peak_allocated = server.stat_peak_memory;
    mh->fragmentation =
        zmalloc_get_fragmentation_ratio(server.resident_set_size);
    mem_total += server.initial_memory_usage;

    mem = 0;
    if (server.repl_backlog)
        mem += zmalloc_size(server.repl_backlog);
    mh->repl_backlog = mem;
    mem_total += mem;

    mem = 0;
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *c = listNodeValue(ln);
            mem += getClientOutputBufferMemoryUsage(c);
            mem += sdsAllocSize(c->querybuf);
            mem += sizeof(client);
        }
    }
    mh->clients_slaves = mem;
    mem_total+=mem;

    mem = 0;
    if (listLength(server.clients)) {
        listIter li;
        listNode *ln;

        listRewind(server.clients,&li);
        while((ln = listNext(&li))) {
            client *c = listNodeValue(ln);
            if (c->flags & CLIENT_SLAVE)
                continue;
            mem += getClientOutputBufferMemoryUsage(c);
            mem += sdsAllocSize(c->querybuf);
            mem += sizeof(client);
        }
    }
    mh->clients_normal = mem;
    mem_total+=mem;

    mem = 0;
    if (server.aof_state != AOF_OFF) {
        mem += sdslen(server.aof_buf);
        mem += aofRewriteBufferSize();
    }
    mh->aof_buffer = mem;
    mem_total+=mem;

    for (j = 0; j < server.dbnum; j++) {
        redisDb *db = server.db+j;
        long long keyscount = dictSize(db->dict);
        if (keyscount==0) continue;

        mh->total_keys += keyscount;
        mh->db = zrealloc(mh->db,sizeof(mh->db[0])*(mh->num_dbs+1));
        mh->db[mh->num_dbs].dbid = j;

        mem = dictSize(db->dict) * sizeof(dictEntry) +
              dictSlots(db->dict) * sizeof(dictEntry*) +
              dictSize(db->dict) * sizeof(robj);
        mh->db[mh->num_dbs].overhead_ht_main = mem;
        mem_total+=mem;

        mem = dictSize(db->expires) * sizeof(dictEntry) +
              dictSlots(db->expires) * sizeof(dictEntry*);
        mh->db[mh->num_dbs].overhead_ht_expires = mem;
        mem_total+=mem;

        mh->num_dbs++;
    }

    mh->overhead_total = mem_total;
    mh->dataset = zmalloc_used - mem_total;
    mh->peak_perc = (float)zmalloc_used*100/mh->peak_allocated;

    /* Metrics computed after subtracting the startup memory from
     * the total memory. */
    size_t net_usage = 1;
    if (zmalloc_used > mh->startup_allocated)
        net_usage = zmalloc_used - mh->startup_allocated;
    mh->dataset_perc = (float)mh->dataset*100/net_usage;
    mh->bytes_per_key = mh->total_keys ? (net_usage / mh->total_keys) : 0;

    return mh;
}

/* Helper for "MEMORY allocator-stats", used as a callback for the jemalloc
 * stats output. */
void inputCatSds(void *result, const char *str) {
    /* result is actually a (sds *), so re-cast it here */
    sds *info = (sds *)result;
    *info = sdscat(*info, str);
}

/* This implements MEMORY DOCTOR. An human readable analysis of the Redis
 * memory condition. */
sds getMemoryDoctorReport(void) {
    int empty = 0;          /* Instance is empty or almost empty. */
    int big_peak = 0;       /* Memory peak is much larger than used mem. */
    int high_frag = 0;      /* High fragmentation. */
    int big_slave_buf = 0;  /* Slave buffers are too big. */
    int big_client_buf = 0; /* Client buffers are too big. */
    int num_reports = 0;
    struct redisMemOverhead *mh = getMemoryOverheadData();

    if (mh->total_allocated < (1024*1024*5)) {
        empty = 1;
        num_reports++;
    } else {
        /* Peak is > 150% of current used memory? */
        if (((float)mh->peak_allocated / mh->total_allocated) > 1.5) {
            big_peak = 1;
            num_reports++;
        }

        /* Fragmentation is higher than 1.4? */
        if (mh->fragmentation > 1.4) {
            high_frag = 1;
            num_reports++;
        }

        /* Clients using more than 200k each average? */
        long numslaves = listLength(server.slaves);
        long numclients = listLength(server.clients)-numslaves;
        if (mh->clients_normal / numclients > (1024*200)) {
            big_client_buf = 1;
            num_reports++;
        }

        /* Slaves using more than 10 MB each? */
        if (numslaves > 0 && mh->clients_slaves / numslaves > (1024*1024*10)) {
            big_slave_buf = 1;
            num_reports++;
        }
    }

    sds s;
    if (num_reports == 0) {
        s = sdsnew(
        "Hi Sam, I can't find any memory issue in your instance. "
        "I can only account for what occurs on this base.\n");
    } else if (empty == 1) {
        s = sdsnew(
        "Hi Sam, this instance is empty or is using very little memory, "
        "my issues detector can't be used in these conditions. "
        "Please, leave for your mission on Earth and fill it with some data. "
        "The new Sam and I will be back to our programming as soon as I "
        "finished rebooting.\n");
    } else {
        s = sdsnew("Sam, I detected a few issues in this Redis instance memory implants:\n\n");
        if (big_peak) {
            s = sdscat(s," * Peak memory: In the past this instance used more than 150% the memory that is currently using. The allocator is normally not able to release memory after a peak, so you can expect to see a big fragmentation ratio, however this is actually harmless and is only due to the memory peak, and if the Redis instance Resident Set Size (RSS) is currently bigger than expected, the memory will be used as soon as you fill the Redis instance with more data. If the memory peak was only occasional and you want to try to reclaim memory, please try the MEMORY PURGE command, otherwise the only other option is to shutdown and restart the instance.\n\n");
        }
        if (high_frag) {
            s = sdscatprintf(s," * High fragmentation: This instance has a memory fragmentation greater than 1.4 (this means that the Resident Set Size of the Redis process is much larger than the sum of the logical allocations Redis performed). This problem is usually due either to a large peak memory (check if there is a peak memory entry above in the report) or may result from a workload that causes the allocator to fragment memory a lot. If the problem is a large peak memory, then there is no issue. Otherwise, make sure you are using the Jemalloc allocator and not the default libc malloc. Note: The currently used allocator is \"%s\".\n\n", ZMALLOC_LIB);
        }
        if (big_slave_buf) {
            s = sdscat(s," * Big slave buffers: The slave output buffers in this instance are greater than 10MB for each slave (on average). This likely means that there is some slave instance that is struggling receiving data, either because it is too slow or because of networking issues. As a result, data piles on the master output buffers. Please try to identify what slave is not receiving data correctly and why. You can use the INFO output in order to check the slaves delays and the CLIENT LIST command to check the output buffers of each slave.\n\n");
        }
        if (big_client_buf) {
            s = sdscat(s," * Big client buffers: The clients output buffers in this instance are greater than 200K per client (on average). This may result from different causes, like Pub/Sub clients subscribed to channels bot not receiving data fast enough, so that data piles on the Redis instance output buffer, or clients sending commands with large replies or very large sequences of commands in the same pipeline. Please use the CLIENT LIST command in order to investigate the issue if it causes problems in your instance, or to understand better why certain clients are using a big amount of memory.\n\n");
        }
        s = sdscat(s,"I'm here to keep you safe, Sam. I want to help you.\n");
    }
    freeMemoryOverheadData(mh);
    return s;
}

/* ======================= The OBJECT and MEMORY commands =================== */

/* This is a helper function for the OBJECT command. We need to lookup keys
 * without any modification of LRU or other parameters. */
robj *objectCommandLookup(client *c, robj *key) {
    dictEntry *de;

    if ((de = dictFind(c->db->dict,key->ptr)) == NULL) return NULL;
    return (robj*) dictGetVal(de);
}

robj *objectCommandLookupOrReply(client *c, robj *key, robj *reply) {
    robj *o = objectCommandLookup(c,key);

    if (!o) addReply(c, reply);
    return o;
}

/* Object command allows to inspect the internals of an Redis Object.
 * Usage: OBJECT <refcount|encoding|idletime|freq> <key> */
void objectCommand(client *c) {
    robj *o;

    if (!strcasecmp(c->argv[1]->ptr,"help") && c->argc == 2) {
        void *blenp = addDeferredMultiBulkLength(c);
        int blen = 0;
        blen++; addReplyStatus(c,
        "OBJECT <subcommand> key. Subcommands:");
        blen++; addReplyStatus(c,
        "refcount -- Return the number of references of the value associated with the specified key.");
        blen++; addReplyStatus(c,
        "encoding -- Return the kind of internal representation used in order to store the value associated with a key.");
        blen++; addReplyStatus(c,
        "idletime -- Return the idle time of the key, that is the approximated number of seconds elapsed since the last access to the key.");
        blen++; addReplyStatus(c,
        "freq -- Return the access frequency index of the key. The returned integer is proportional to the logarithm of the recent access frequency of the key.");
        setDeferredMultiBulkLength(c,blenp,blen);
    } else if (!strcasecmp(c->argv[1]->ptr,"refcount") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyLongLong(c,o->refcount);
    } else if (!strcasecmp(c->argv[1]->ptr,"encoding") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        addReplyBulkCString(c,strEncoding(o->encoding));
    } else if (!strcasecmp(c->argv[1]->ptr,"idletime") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            addReplyError(c,"An LFU maxmemory policy is selected, idle time not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return;
        }
        addReplyLongLong(c,estimateObjectIdleTime(o)/1000);
    } else if (!strcasecmp(c->argv[1]->ptr,"freq") && c->argc == 3) {
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        if (!(server.maxmemory_policy & MAXMEMORY_FLAG_LFU)) {
            addReplyError(c,"An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.");
            return;
        }
        /* LFUDecrAndReturn should be called
         * in case of the key has not been accessed for a long time,
         * because we update the access time only
         * when the key is read or overwritten. */
        addReplyLongLong(c,LFUDecrAndReturn(o));
    } else {
        addReplyErrorFormat(c, "Unknown subcommand or wrong number of arguments for '%s'. Try OBJECT help",
            (char *)c->argv[1]->ptr);
    }
}

/* The memory command will eventually be a complete interface for the
 * memory introspection capabilities of Redis.
 *
 * Usage: MEMORY usage <key> */
void memoryCommand(client *c) {
    robj *o;

    if (!strcasecmp(c->argv[1]->ptr,"usage") && c->argc >= 3) {
        long long samples = OBJ_COMPUTE_SIZE_DEF_SAMPLES;
        for (int j = 3; j < c->argc; j++) {
            if (!strcasecmp(c->argv[j]->ptr,"samples") &&
                j+1 < c->argc)
            {
                if (getLongLongFromObjectOrReply(c,c->argv[j+1],&samples,NULL)
                     == C_ERR) return;
                if (samples < 0) {
                    addReply(c,shared.syntaxerr);
                    return;
                }
                if (samples == 0) samples = LLONG_MAX;;
                j++; /* skip option argument. */
            } else {
                addReply(c,shared.syntaxerr);
                return;
            }
        }
        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nullbulk))
                == NULL) return;
        size_t usage = objectComputeSize(o,samples);
        usage += sdsAllocSize(c->argv[2]->ptr);
        usage += sizeof(dictEntry);
        addReplyLongLong(c,usage);
    } else if (!strcasecmp(c->argv[1]->ptr,"stats") && c->argc == 2) {
        struct redisMemOverhead *mh = getMemoryOverheadData();

        addReplyMultiBulkLen(c,(14+mh->num_dbs)*2);

        addReplyBulkCString(c,"peak.allocated");
        addReplyLongLong(c,mh->peak_allocated);

        addReplyBulkCString(c,"total.allocated");
        addReplyLongLong(c,mh->total_allocated);

        addReplyBulkCString(c,"startup.allocated");
        addReplyLongLong(c,mh->startup_allocated);

        addReplyBulkCString(c,"replication.backlog");
        addReplyLongLong(c,mh->repl_backlog);

        addReplyBulkCString(c,"clients.slaves");
        addReplyLongLong(c,mh->clients_slaves);

        addReplyBulkCString(c,"clients.normal");
        addReplyLongLong(c,mh->clients_normal);

        addReplyBulkCString(c,"aof.buffer");
        addReplyLongLong(c,mh->aof_buffer);

        for (size_t j = 0; j < mh->num_dbs; j++) {
            char dbname[32];
            snprintf(dbname,sizeof(dbname),"db.%zd",mh->db[j].dbid);
            addReplyBulkCString(c,dbname);
            addReplyMultiBulkLen(c,4);

            addReplyBulkCString(c,"overhead.hashtable.main");
            addReplyLongLong(c,mh->db[j].overhead_ht_main);

            addReplyBulkCString(c,"overhead.hashtable.expires");
            addReplyLongLong(c,mh->db[j].overhead_ht_expires);
        }

        addReplyBulkCString(c,"overhead.total");
        addReplyLongLong(c,mh->overhead_total);

        addReplyBulkCString(c,"keys.count");
        addReplyLongLong(c,mh->total_keys);

        addReplyBulkCString(c,"keys.bytes-per-key");
        addReplyLongLong(c,mh->bytes_per_key);

        addReplyBulkCString(c,"dataset.bytes");
        addReplyLongLong(c,mh->dataset);

        addReplyBulkCString(c,"dataset.percentage");
        addReplyDouble(c,mh->dataset_perc);

        addReplyBulkCString(c,"peak.percentage");
        addReplyDouble(c,mh->peak_perc);

        addReplyBulkCString(c,"fragmentation");
        addReplyDouble(c,mh->fragmentation);

        freeMemoryOverheadData(mh);
    } else if (!strcasecmp(c->argv[1]->ptr,"malloc-stats") && c->argc == 2) {
#if defined(USE_JEMALLOC)
        sds info = sdsempty();
        je_malloc_stats_print(inputCatSds, &info, NULL);
        addReplyBulkSds(c, info);
#else
        addReplyBulkCString(c,"Stats not supported for the current allocator");
#endif
    } else if (!strcasecmp(c->argv[1]->ptr,"doctor") && c->argc == 2) {
        sds report = getMemoryDoctorReport();
        addReplyBulkSds(c,report);
    } else if (!strcasecmp(c->argv[1]->ptr,"purge") && c->argc == 2) {
#if defined(USE_JEMALLOC)
        char tmp[32];
        unsigned narenas = 0;
        size_t sz = sizeof(unsigned);
        if (!je_mallctl("arenas.narenas", &narenas, &sz, NULL, 0)) {
            sprintf(tmp, "arena.%d.purge", narenas);
            if (!je_mallctl(tmp, NULL, 0, NULL, 0)) {
                addReply(c, shared.ok);
                return;
            }
        }
        addReplyError(c, "Error purging dirty pages");
#else
        addReply(c, shared.ok);
        /* Nothing to do for other allocators. */
#endif
    } else if (!strcasecmp(c->argv[1]->ptr,"help") && c->argc == 2) {
        addReplyMultiBulkLen(c,5);
        addReplyBulkCString(c,
"MEMORY DOCTOR                        - Outputs memory problems report");
        addReplyBulkCString(c,
"MEMORY USAGE <key> [SAMPLES <count>] - Estimate memory usage of key");
        addReplyBulkCString(c,
"MEMORY STATS                         - Show memory usage details");
        addReplyBulkCString(c,
"MEMORY PURGE                         - Ask the allocator to release memory");
        addReplyBulkCString(c,
"MEMORY MALLOC-STATS                  - Show allocator internal stats");
    } else {
        addReplyError(c,"Syntax error. Try MEMORY HELP");
    }
}













