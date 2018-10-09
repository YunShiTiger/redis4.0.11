/*
 * Redis中的hash结构实现与相关命令
 */

#include "server.h"
#include <math.h>

/*---------------------------------------------------------------------------
 * Hash对象结构中的相关函数
 * Hash type API
 *---------------------------------------------------------------------------
 */

/* 检测新引入的字段和值是否会引起hash对象底层实现结构的变化---->主要是检测给定的字段和值的长度是否超过了预设值
 * Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. 
 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;
	//检测当前hash对象的编码是否是ziplist形式
    if (o->encoding != OBJ_ENCODING_ZIPLIST) 
		return;

    //循环检测输入的字段和值的内容长度是否超出了预设值
    for (i = start; i <= end; i++) {
        if (sdsEncodedObject(argv[i]) && sdslen(argv[i]->ptr) > server.hash_max_ziplist_value) {
			//触发进行转换底层实现结构的处理
            hashTypeConvert(o, OBJ_ENCODING_HT);
            break;
        }
    }
}

/* 检测对应的字段是否在ziplist实现的hash对象中
 * Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. 
 */
int hashTypeGetFromZiplist(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);
	//获取对象中ziplist指向
    zl = o->ptr;
	//获取对应的头元素节点位置指向
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
	//检测对应的头节点是否存在
    if (fptr != NULL) {
		//从头结点开始想后进行查询,注意每次进行跳过一个元素,原因是字段和值分别占据一个位置,因此需要跳过对应的值的位置
        fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
		//检测是否找到对应的节点位置--->上述函数的返回值是对应的找到位置的指向
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
		    //获取对应字段所对应的值的指向
            vptr = ziplistNext(zl, fptr);
            serverAssert(vptr != NULL);
        }
    }

    //检测是否找到对应字段的值内容节点的指向
    if (vptr != NULL) {
		//获取对应的值的内容指向
        ret = ziplistGet(vptr, vstr, vlen, vll);
        serverAssert(ret);
	    //返回本字段存在的标识
        return 0;
    }
    //返回没有找到对应的字段的标识
    return -1;
}

/* 检测对应的字段是否在hash表实现的hash对象中
 * Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value is returned. 
 */
sds hashTypeGetFromHashTable(robj *o, sds field) {
    dictEntry *de;

    serverAssert(o->encoding == OBJ_ENCODING_HT);
	//在对应的hash表结构中进行查找是否存在本字段
    de = dictFind(o->ptr, field);
	//检测是否找到对应的字段
    if (de == NULL) 
		//返回没有找到对应字段的空标识
		return NULL;
	//检测获取对应字段的值内容
    return dictGetVal(de);
}

/* 在对应的hash对象中查找对应字段所对应的值的信息
 * Higher level function of hashTypeGet*() that returns the hash value
 * associated with the specified field. If the field is found C_OK
 * is returned, otherwise C_ERR. The returned object is returned by
 * reference in either *vstr and *vlen if it's returned in string form,
 * or stored in *vll if it's returned as a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * for C_OK and checking if vll (or vstr) is NULL. 
 */
int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    //根据hash对象的底层不同实现进行区分处理
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
		//在对应的ziplist中获取对应的字段所对应的值
        if (hashTypeGetFromZiplist(o, field, vstr, vlen, vll) == 0)
			//返回找到对应字段对应值的成功标识
            return C_OK;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value;
		//在对应的hash表中获取对应的字段所对应的值
        if ((value = hashTypeGetFromHashTable(o, field)) != NULL) {
			//设置对应的值的内容指向
            *vstr = (unsigned char*) value;
			//获取对应的字符串的长度
            *vlen = sdslen(value);
		    //返回找到对应的字段所对应的值的成功标识
            return C_OK;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
	//返回没有找到字段的错误标识
    return C_ERR;
}

/* 根据给定的字段获取对应的值对象
 * Like hashTypeGetValue() but returns a Redis object, which is useful for
 * interaction with the hash type outside t_hash.c.
 * The function returns NULL if the field is not found in the hash. Otherwise
 * a newly allocated string object with the value is returned. 
 */
robj *hashTypeGetValueObject(robj *o, sds field) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;
	//获取字段对应的值信息
    if (hashTypeGetValue(o,field,&vstr,&vlen,&vll) == C_ERR) 
		return NULL;
	//检测是否是字符串类型数据
    if (vstr) 
		//创建对应的字符串类型数据
		return createStringObject((char*)vstr,vlen);
    else 
		//根据整数创建对应的字符串类型数据
		return createStringObjectFromLongLong(vll);
}

/* 获取给定字段所对应的值的字符串长度值
 * Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not exist. 
 */
size_t hashTypeGetValueLength(robj *o, sds field) {
    //初始化对应的长度值
    size_t len = 0;
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;
		//在ziplist中获取对应的字段所对应的值数据信息
        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0)
			//计算对应的长度值---->如果是整数的时候,返回的是对应的10进制对应的字符串长度值
            len = vstr ? vlen : sdigits10(vll);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds aux;
		//在hash表中获取对应字段的节点信息
        if ((aux = hashTypeGetFromHashTable(o, field)) != NULL)
			//获取对应的数据长度
            len = sdslen(aux);
    } else {
        serverPanic("Unknown hash encoding");
    }
	//返回数据长度------>没有对应的字段,返回默认的0值
    return len;
}

/* 测试对应的字段是否在hash对象中
 * Test if the specified field exists in the given hash. 
 * Returns 1 if the field exists, and 0 when it doesn't. 
 */
int hashTypeExists(robj *o, sds field) {
    //根据hash底层的不同实现进行相关操作
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;
		//检测对应的字段是否在ziplist中
        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) 
			return 1;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        //检测对应的字段是否在hash表结构中
        if (hashTypeGetFromHashTable(o, field) != NULL) 
			return 1;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return 0;
}

/* 新hash对象中插入或者覆盖对应字段和值，插入返回0 更新返回1
 * Add a new field, overwrite the old with the new value if it already exists.
 * Return 0 on insert and 1 on update.
 *
 * By default, the key and value SDS strings are copied if needed, so the
 * caller retains ownership of the strings passed. However this behavior
 * can be effected by passing appropriate flags (possibly bitwise OR-ed):
 *
 * HASH_SET_TAKE_FIELD -- The SDS field ownership passes to the function.
 * HASH_SET_TAKE_VALUE -- The SDS value ownership passes to the function.
 *
 * When the flags are used the caller does not need to release the passed
 * SDS string(s). It's up to the function to use the string to create a new
 * entry or to free the SDS string before returning to the caller.
 *
 * HASH_SET_COPY corresponds to no flags passed, and means the default
 * semantics of copying the values if needed.
 *
 */
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0

int hashTypeSet(robj *o, sds field, sds value, int flags) {
    int update = 0;
	//根据hash对象的不同编码方式来进行区分对待
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;
		//获取hash对象对应的ziplist指向
        zl = o->ptr;
	    //获取对应的头节点
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
		//检测对应的头节点是否存在
        if (fptr != NULL) {
			//从头结点开始向后查询是否有对应的字段值
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
			//检测是否找到对应的字段值对应的节点
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
			    //获取对应的值节点的指向
                vptr = ziplistNext(zl, fptr);
                serverAssert(vptr != NULL);
			    //设置为进行更新操作标识
                update = 1;

                /* Delete value */
				//进行删除对应的原始值节点内容
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
				//将新的值节点内容设置到ziplist中
                zl = ziplistInsert(zl, vptr, (unsigned char*)value, sdslen(value));
            }
        }

        //检测是否是更新操作------>不是就说明是需要进行插入操作处理了
        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
		    //插入对应的字段
            zl = ziplistPush(zl, (unsigned char*)field, sdslen(field), ZIPLIST_TAIL);
			//插入对应的值
            zl = ziplistPush(zl, (unsigned char*)value, sdslen(value), ZIPLIST_TAIL);
        }
		//重新设置hash对象中ziplist的指向
        o->ptr = zl;

        /* Check if the ziplist needs to be converted to a hash table */
		//检测新添加的字段和值是否引起了hash对象元素数量大于预设值
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
			//进行结构变化操作处理
            hashTypeConvert(o, OBJ_ENCODING_HT);
		
    } else if (o->encoding == OBJ_ENCODING_HT) {
        //在hash表中查询是否有对应字段的信息结构节点
        dictEntry *de = dictFind(o->ptr,field);
		//检测在hash表中是否找到此对应的信息节点
        if (de) {
			//释放在hash表中字段对应的原始值字符串占据的空间
            sdsfree(dictGetVal(de));
            if (flags & HASH_SET_TAKE_VALUE) {
                dictGetVal(de) = value;
				//置空原始的字符串指向
                value = NULL;
            } else {
				//拷贝一份值字符串内容,然后赋值到对应的字段所对应的值上
                dictGetVal(de) = sdsdup(value);
            }
			//设置是更新操作标记
            update = 1;
        } else {
            sds f,v;
            if (flags & HASH_SET_TAKE_FIELD) {
                f = field;
                field = NULL;
            } else {
				//拷贝了一份对应的字段字符串
                f = sdsdup(field);
            }
			//根据标记来确定是否进行对原始值字符串对象空间释放操作处理
            if (flags & HASH_SET_TAKE_VALUE) {
				//记录对原始值字符串的指向
                v = value;
				//设置原始的值字符串指向置空---->即需要原始调用者对应值字符串占据的空间进行释放处理
                value = NULL;
            } else {
				//拷贝了一份对应的值字符串
                v = sdsdup(value);
            }
			//将对应的字段和值插入到hash表中
            dictAdd(o->ptr,f,v);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

    /* Free SDS strings we did not referenced elsewhere if the flags want this function to be responsible. */
	//根据配置参数来进一步确定是否需要进行字符串空间的释放操作处理
	//注意这里的处理好像只有在ziplist中才回引发处理------>如果是对应的hash表,内部已经做了处理
    if (flags & HASH_SET_TAKE_FIELD && field) 
		//释放对应的原始字段字符串占据的空间
		sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) 
		//释放对应的原始值字符串占据的空间
		sdsfree(value);
	//返回是否进行更新还是插入标识
    return update;
}

/* 在hash对象中删除给定的字段值
 * Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. 
 */
int hashTypeDelete(robj *o, sds field) {
    int deleted = 0;
	//根据hash对象的编码方式进行分别处理
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;
		//获取ziplist结构指向
        zl = o->ptr;
	    //获取对应的头节点元素指向
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
		//检测是否存在对应的头节点指向
        if (fptr != NULL) {
			//从头节点开始向后遍历,查找对应字段相同的节点
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
			//检测是否找到对应的字段节点
            if (fptr != NULL) {
				//删除对应的字段节点
                zl = ziplistDelete(zl,&fptr); /* Delete the key. */
				//删除对应的值内容节点
                zl = ziplistDelete(zl,&fptr); /* Delete the value. */
			    //设置对应的hash结构的真实数据指向位置
                o->ptr = zl;
				//设置进行删除操作标记
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        //在对应的hash表中删除对应的字段
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
			//设置删除标识
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
		    //删除一个元素后,检测是否需要进行重新设置空间分配处理
            if (htNeedsResize(o->ptr)) 
				//进行空间扩容操作处理
				dictResize(o->ptr);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
	//返回是否删除对应的字段和值的标识
    return deleted;
}

/* 获取hash对象中元素的数量
 * Return the number of elements in a hash. 
 */
unsigned long hashTypeLength(const robj *o) {
    unsigned long length = ULONG_MAX;
	//根据hash对象的不同实现来处理对应的数量获取问题
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
		//获取ziplist中总的元素数量,计算一半为对应hash对象的元素数量
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        //获取hash表结构中元素的数量
        length = dictSize((const dict*)o->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
	//返回对应的hash对象元素值
    return length;
}

/* 根据给定的hash对象创建对应的迭代器对象 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {
    //分配对应的空间
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
	//设置需要迭代的对象
    hi->subject = subject;
	//设置对象对应的编码方式
    hi->encoding = subject->encoding;
	//根据编码方式不同初始化对应的参数
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
		//设置迭代hash表的迭代器
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
	//返回构建后的迭代器指向
    return hi;
}

/* 释放对应的迭代器空间 */
void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT)
		//如果hash对象是hash表类型,这个地方需要释放对应的hash表的迭代器占据的空间
        dictReleaseIterator(hi->di);
	//释放hash对象对应的迭代器空间
    zfree(hi);
}

/* 根据给定的迭代器对象在hash对象中获取下一个需要进行遍历的元素
 * Move to the next entry in the hash. Return C_OK when the next entry
 * could be found and C_ERR when the iterator reaches the end. 
 */
int hashTypeNext(hashTypeIterator *hi) {
    //根据编码方式来进行分别处理
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;
	
	    //获取对应的ziplist指向
        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            serverAssert(vptr == NULL);
			//设置本指针指向第一个需要遍历的元素
            fptr = ziplistIndex(zl, 0);
        } else {
            /* Advance cursor */
            serverAssert(vptr != NULL);
			//获取下一个需要进行遍历的元素
            fptr = ziplistNext(zl, vptr);
        }
		//检测是否还有能够进行遍历的元素
        if (fptr == NULL) 
			//返回没有对应的元素能够进行遍历的标识
			return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
		//在有对应的元素能够遍历的情况下,获取对应的值的指向
        vptr = ziplistNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
		//更新迭代器中的相关指针的指向
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
		//在hash表中获取下一个能够遍历的元素节点信息
        if ((hi->de = dictNext(hi->di)) == NULL) 
			//在没有下一个元素的情况下,返回对应的没有下一个元素节点的标识
			return C_ERR;
    } else {
        serverPanic("Unknown hash encoding");
    }
	//返回还能够向下进行遍历的标识
    return C_OK;
}

/* 根据提供的迭代器状态从对应的ziplist中获取对应的当前需要遍历的节点信息
 * Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. 
 */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    int ret;

    serverAssert(hi->encoding == OBJ_ENCODING_ZIPLIST);

    if (what & OBJ_HASH_KEY) {
		//获取对应的字段值的数据
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        serverAssert(ret);
    } else {
		//获取对应的值的数据
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        serverAssert(ret);
    }
}

/* 根据提供的迭代器状态从对应的hash表中获取对应的当前需要遍历的节点信息
 * Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hash table. Prototype is similar to
 * `hashTypeGetFromHashTable`. 
 */
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);

    if (what & OBJ_HASH_KEY) {
		//获取对应的节点的字段值
        return dictGetKey(hi->de);
    } else {
        //获取对应的节点的值
        return dictGetVal(hi->de);
    }
}

/* 根据提供的迭代器来获取当前位置上的字段或者字段对应的值的数据
 * Higher level function of hashTypeCurrent*() that returns the hash value
 * at current iterator position.
 *
 * The returned element is returned by reference in either *vstr and *vlen if
 * it's returned in string form, or stored in *vll if it's returned as
 * a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * type checking if vstr == NULL. 
 */
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    //根据当前的编码类型来触发不同的获取数据方式
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
		//获取对应的ziplist上的数据
        hashTypeCurrentFromZiplist(hi, what, vstr, vlen, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
		//获取对应的hash表上对应的数据
        sds ele = hashTypeCurrentFromHashTable(hi, what);
		//配置返回的数据参数
        *vstr = (unsigned char*) ele;
        *vlen = sdslen(ele);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* 根据提供的迭代器来获取当前位置上的字段或者字段对应的值的数据,并根据对应的数据创建新的字符串数据
 * Return the key or value at the current iterator position as a new SDS string. 
 */
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

	//根据迭代器获取对应位置上的元素数据
    hashTypeCurrentObject(hi,what,&vstr,&vlen,&vll);
	//检测是否是字符串类型的数据
    if (vstr) 
		//创建对应的字符串类型数据
		return sdsnewlen(vstr,vlen);
	//根据整数来创建对应的字符串对象
    return sdsfromlonglong(vll);
}

/* 检测对应键所对应的hash对象是否在redis中,，没有就进行创建操作处理,类型不对向客户端进行错误类型响应 */
robj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    //检测键所对应的hash对象是否存在
    robj *o = lookupKeyWrite(c->db,key);
    if (o == NULL) {
		//创建对应的hash对象
        o = createHashObject();
		//将对应的键值对结构放置到redis中
        dbAdd(c->db,key,o);
    } else {
		//检测对应的值对象是否是hash类型的对象
        if (o->type != OBJ_HASH) {
			//向客户端返回类型错误的响应
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
	//返回找到或者新建的hash对象
    return o;
}

/* 实现将ziplist结构数据转化成对应的hash表结构数据 */
void hashTypeConvertZiplist(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    if (enc == OBJ_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_HT) {
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        //创建对应的迭代器对象
        hi = hashTypeInitIterator(o);
		//创建对应的hash表结构,用于存储对应的数据
        dict = dictCreate(&hashDictType, NULL);

        //循环遍历老数据,将ziplist结构中的数据转化成hash表结构上的数据
        while (hashTypeNext(hi) != C_ERR) {
            sds key, value;
			//获取对应的字段值
            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
	        //获取字段对应的值数据
            value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
			//将字段和值插入到新的hash表结构中
            ret = dictAdd(dict, key, value);
			//检测是否进行插入数据成功
            if (ret != DICT_OK) {
                serverLogHexDump(LL_WARNING,"ziplist with dup elements dump",o->ptr,ziplistBlobLen(o->ptr));
                serverPanic("Ziplist corruption detected");
            }
        }
		//操作完成后,是否迭代器占据的空间
        hashTypeReleaseIterator(hi);
		//释放hash对象中元素存储ziplist结构的数据空间
        zfree(o->ptr);
		//设置hash对象新的编码方式为hash表结构
        o->encoding = OBJ_ENCODING_HT;
		//设置hash对象真实的数据指向
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* 将对应的ziplist结构的hash对象转换成hash表结构的hash对象*/
void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
		//实现从ziplist转换成hash表的结构变化
        hashTypeConvertZiplist(o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*---------------------------------------------------------------------------
 * hash结构的相关命令
 * Hash type commands
 *---------------------------------------------------------------------------
 */

/* 用于为哈希表中不存在的的字段赋值
 *     如果哈希表不存在，一个新的哈希表被创建并进行 HSET 操作。
 *     如果字段已经存在于哈希表中，操作无效
 *     如果 key 不存在，一个新哈希表被创建并执行 HSETNX 命令。 
 * 命令格式
 *     HSETNX KEY_NAME FIELD VALUE
 * 返回值
 *     设置成功，返回 1。 如果给定字段已经存在且没有操作被执行，返回 0
 */
void hsetnxCommand(client *c) {
    robj *o;
	//检测键所对应的hash对象是否存在并进行创建操作处理
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) 
		return;
	//检测新引入的字段和值的内容是否引起本hash对象底层实现的变换----->即ziplist转换成hash表
    hashTypeTryConversion(o,c->argv,2,3);
	//检测对应的字段是否已经在hash结构对象中
    if (hashTypeExists(o, c->argv[2]->ptr)) {
		//向客户端返回字段已经存在的响应
        addReply(c, shared.czero);
    } else {
        //将对应的字段和值设置到hash对象上
        hashTypeSet(o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
		//向客户端返回设置成功的响应
        addReply(c, shared.cone);
		//发送键值对变化的信号
        signalModifiedKey(c->db,c->argv[1]);
		//发送触发对应命令的通知
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
		//增加脏计数值
        server.dirty++;
    }
}

/* 用于为哈希表中的字段赋值 
 *     如果哈希表不存在，一个新的哈希表被创建并进行 HSET 操作
 *     如果字段已经存在于哈希表中，旧值将被覆盖
 *
 * 命令格式
 *     HSET KEY_NAME FIELD VALUE ...
 * 返回值
 *     如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。 如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0 。
 */
void hsetCommand(client *c) {
    int i, created = 0;
    robj *o;
    //检测客户端对应的传入参数是否是对应的奇数个参数
    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }
	//获取对应键所对应的hash类型对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) 
		return;
	//进行检测是否需要进行hash对象底层结构变化
    hashTypeTryConversion(o,c->argv,2,c->argc-1);

    //循环进行字段和值的插入操作处理
    for (i = 2; i < c->argc; i += 2)
		//进行插入字段和值的操作处理
        created += !hashTypeSet(o,c->argv[i]->ptr,c->argv[i+1]->ptr,HASH_SET_COPY);

    /* HMSET (deprecated) and HSET return value is different. */
	//获取客户端传入的命令参数
    char *cmdname = c->argv[0]->ptr;
	//检测是多字段插入还是单字段插入
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
	    //单字段插入,返回插入数量值
        addReplyLongLong(c, created);
    } else {
        /* HMSET */
	    //多字段插入,返回插入成功标识响应
        addReply(c, shared.ok);
    }
	//发送键值对空间变化信号
    signalModifiedKey(c->db,c->argv[1]);
	//发送执行对应命令通知
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
	//增加脏数据计数值
    server.dirty++;
}

/* 用于为哈希表中的字段值加上指定增量值
 *     增量也可以为负数，相当于对指定字段进行减法操作
 *     如果哈希表的 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令
 *     如果指定的字段不存在，那么在执行命令前，字段的值被初始化为 0
 *     对一个储存字符串值的字段执行 HINCRBY 命令将造成一个错误
 *     本操作的值被限制在 64 位(bit)有符号数字表示之内
 * 命令格式
 *     HINCRBY KEY_NAME FIELD_NAME INCR_BY_NUMBER
 * 返回值
 *     执行 HINCRBY 命令之后，哈希表中字段的值
 *
 *   通过分析对应的处理过程,发现在这个命令中出现了两次进行遍历处理(如果是ziplist的话,效率会比较低,所以数据元素多了的话,需要升级为hash表结构)
 *         1 首先查找对应字段所对应的老值
 *         2 将新值再次插入到hash对象中
 */
void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;
	//获取对应的增量参数值
    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) 
		return;
	//检测或创建对应的hash对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) 
		return;
	//查找对应字段所对应的值信息
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&value) == C_OK) {
		//监测找到的值是否是字符串类型数据
        if (vstr) {
			//尝试进行将字符串类型数据转换成整数数据
            if (string2ll((char*)vstr,vlen,&value) == 0) {
				//转换失败,向客户端返回不能在字符串上进行增加指定数量值的处理
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else {
        //没有对应字段,默认设置为0
        value = 0;
    }

    //记录对应的老值
    oldvalue = value;
	//检测增加对应值之后是否出现越界错误
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) || (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
		//向客户端返回越界错误响应
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
	//计算对应的新值
    value += incr;
	//获取数值对应的字符串对象
    new = sdsfromlonglong(value);
	//将对应的新值设置到hash对象中
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
	//向客户端返回增加增量后的值
    addReplyLongLong(c,value);
	//发送键值对变化信号
    signalModifiedKey(c->db,c->argv[1]);
	//发送执行命令通知
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
	//增加脏数据计数值
    server.dirty++;
}

/*
 * 用于为哈希表中的字段值加上指定浮点数增量值
 *     如果指定的字段不存在，那么在执行命令前，字段的值被初始化为 0 
 * 命令格式
 *     HINCRBYFLOAT KEY_NAME FIELD_NAME INCR_BY_NUMBER
 * 返回值
 *     执行 Hincrbyfloat 命令之后，哈希表中字段的值
 */
void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;
	
	//获取对应的增量参数值
    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) 
		return;
	
	//检测或创建对应的hash对象
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) 
		return;
	
	//查找对应字段所对应的值信息
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&ll) == C_OK) {
		//检测对应的数据是否是字符串类型
        if (vstr) {
			//进行尝试转换操作处理
            if (string2ld((char*)vstr,vlen,&value) == 0) {
				//向客户端返回数据类型不能进行增量操作处理
                addReplyError(c,"hash value is not a float");
                return;
            }
        } else {
            //记录对应的double数据值
            value = (long double)ll;
        }
    } else {
        //在没有对应字段的情况下设置一个默认的0值
        value = 0;
    }

    //进行增量操作处理
    value += incr;

    //创建一个足够大的空间
    char buf[MAX_LONG_DOUBLE_CHARS];
	//将对应的数值数据转换成字符串类型
    int len = ld2string(buf,sizeof(buf),value,1);
	//创建对应的字符串类型对象
    new = sdsnewlen(buf,len);
	//进行将新的数据插入到对应的hash对象中
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
	//向客户端返回进行增量操作后的数值
    addReplyBulkCBuffer(c,buf,len);
	//发送键值对空间变化信号
    signalModifiedKey(c->db,c->argv[1]);
	//发送进行操作相关命令的通知
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
	//增加脏数据计数值
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. 
     */
    //用HSET命令代替HINCRBYFLOAT，以防不同的浮点精度造成的误差
    
    robj *aux, *newobj;
	//创建HSET字符串对象
    aux = createStringObject("HSET",4);
    newobj = createRawStringObject(buf,len);
	//修改HINCRBYFLOAT命令为HSET对象
    rewriteClientCommandArgument(c,0,aux);
	//释放空间
    decrRefCount(aux);
	//修改increment为新的值对象new
    rewriteClientCommandArgument(c,3,newobj);
	//释放空间
    decrRefCount(newobj);
}

/* 获取hash对象中指定字段所对应的值 */
static void addHashFieldToReply(client *c, robj *o, sds field) {
    int ret;

    //进一步检测对应的hash对象是否存在
    if (o == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    //根据hash对象的不同编码方式进行获取给定字段所对应的值
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;
		//在ziplist找查找对应字段对应的值
        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
		//检测是否查找到对应的字段
        if (ret < 0) {
			//向客户端返回不存在的响应
            addReply(c, shared.nullbulk);
        } else {
            //检测获取到的值的内容是否是字符串类型数据
            if (vstr) {
				//向客户端返回对应的字符串数据
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                //向客户端返回对应的整数数据
                addReplyBulkLongLong(c, vll);
            }
        }

    } else if (o->encoding == OBJ_ENCODING_HT) {
        //在hash表找查找对应字段对应的值
        sds value = hashTypeGetFromHashTable(o, field);
		//检测对应的值是否存在
        if (value == NULL)
			//向客户端返回不存在的响应
            addReply(c, shared.nullbulk);
        else
			//向客户端返回对应的值的内容
            addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*
 * 用于返回哈希表中指定字段的值
 * 命令格式
 *     HGET KEY_NAME FIELD_NAME
 * 返回值
 *     返回给定字段的值。如果给定的字段或 key 不存在时，返回 nil
 */
void hgetCommand(client *c) {
    robj *o;
	//检测给定键所对应的对象是否存在,且对应的对象是否是hash类型
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL || checkType(c,o,OBJ_HASH)) 
		return;
	//获取给定字段所对应的值
    addHashFieldToReply(c, o, c->argv[2]->ptr);
}

/*
 * 用于返回哈希表中，一个或多个给定字段的值
 *     如果指定的字段不存在于哈希表，那么返回一个 nil 值
 * 命令格式
 *     HMGET KEY_NAME FIELD1...FIELDN 
 * 返回值
 *     一个包含多个给定字段关联值的表，表值的排列顺序和指定字段的请求顺序一样
 */
void hmgetCommand(client *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty hashes, where HMGET should respond with a series of null bulks. */
    //获取对应键所对应的值对象
	o = lookupKeyRead(c->db, c->argv[1]);
	//检测对应值对象类型是否是hash类型
    if (o != NULL && o->type != OBJ_HASH) {
		//返回类型错误响应
        addReply(c, shared.wrongtypeerr);
        return;
    }
	//创建对应大小的返回值的空间
    addReplyMultiBulkLen(c, c->argc-2);
	//循环处理,设置对应的每个字段的返回值的内容
    for (i = 2; i < c->argc; i++) {
		//触发获取对应的各个字段所对应的值
        addHashFieldToReply(c, o, c->argv[i]->ptr);
    }
}

/*
 * 用于删除哈希表 key 中的一个或多个指定字段，不存在的字段将被忽略。
 * 命令格式
 *     HDEL KEY_NAME FIELD1.. FIELDN
 * 返回值
 *     被成功删除字段的数量，不包括被忽略的字段
 */
void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;
	
	//检测对应键所对应的值对象是否存在,且是否是hash类型的对象
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL || checkType(c,o,OBJ_HASH)) 
		return;

    //循环处理,删除客户端指定的字段内容
    for (j = 2; j < c->argc; j++) {
		//检测删除对应字段是否成功
        if (hashTypeDelete(o,c->argv[j]->ptr)) {
			//增加删除字段数量值
            deleted++;
			//检测当前hash对象中是否还用元素
            if (hashTypeLength(o) == 0) {
				//在redis中删除当前对应的键值对
                dbDelete(c->db,c->argv[1]);
				//设置删除键值对标识
                keyremoved = 1;
			    //退出删除循环
                break;
            }
        }
    }
	//检测是否删除过对应的字段
    if (deleted) {
		//发送键值对空间内容变化信号
        signalModifiedKey(c->db,c->argv[1]);
		//发送进行相关命令的通知
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
	    //检测是否删除了键值对
        if (keyremoved)
			//发送删除对应键值对的通知
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
		//增加脏数据计数值
        server.dirty += deleted;
    }
	//向客户端返回删除对应字段的数量值
    addReplyLongLong(c,deleted);
}

/*
 * 用于获取哈希表中字段的数量
 * 命令格式
 *     HLEN KEY_NAME
 * 返回值
 *     哈希表中字段的数量。 当 key 不存在时，返回 0 。
 */
void hlenCommand(client *c) {
    robj *o;
	//检测对应键所对应的值对象是否存在,且是否是hash类型的对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL || checkType(c,o,OBJ_HASH)) 
		return;
	//向客户端返回hash对象中元素的数量
    addReplyLongLong(c,hashTypeLength(o));
}

/*
 * 获取哈希表 key 中， 与给定域 field 相关联的值的字符串长度
 *     如果给定的键或者域不存在， 那么命令返回 0
 * 命令格式
 *     HSTRLEN KEY_NAME FIELD1
 * 返回值
 *     用于表示对应字段所对应值的长度
 */
void hstrlenCommand(client *c) {
    robj *o;
	//检测对应键所对应的值对象是否存在,且是否是hash类型的对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL || checkType(c,o,OBJ_HASH)) 
		return;
	//返回对应字段所对应值的长度值
    addReplyLongLong(c,hashTypeGetValueLength(o,c->argv[2]->ptr));
}

/* 根据对应的迭代器和标识来获取迭代器中记录的需要遍历的数据 */
static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    //根据编码方式进行区分操作
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;
		//在ziplist中获取当前迭代器指向的数据
        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
		//检测对应的数据是字符串类型还是整数类型
        if (vstr)
			//将字符串类型数据填充到响应集合中
            addReplyBulkCBuffer(c, vstr, vlen);
        else
			//将整数类型数据填充到响应集合中-------------->因为在ziplist中实现了进行将可以进行整数编码处理的字符串数据进行了整数编码处理
            addReplyBulkLongLong(c, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        //在hash表中获取当前迭代器指向的数据
        sds value = hashTypeCurrentFromHashTable(hi, what);
		//将字符串类型数据填充到响应集合中---------------->这个地方hash结构中并没有进行对字符串数据进行整数编码处理操作
        addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* 统一的根据对应标识来获取hash对象中的相关数据信息的处理函数 */
void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int multiplier = 0;
    int length, count = 0;
	
	//检测对应键所对应的值对象是否存在,且是否是hash类型的对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL || checkType(c,o,OBJ_HASH)) 
		return;

    //根据标识来确定需要创建空间的倍数值
    if (flags & OBJ_HASH_KEY) 
		multiplier++;
    if (flags & OBJ_HASH_VALUE) 
		multiplier++;
	
    //计算需要返回客户端元素的个数值
    length = hashTypeLength(o) * multiplier;
	//开辟对应数量的空间
    addReplyMultiBulkLen(c, length);
	//创建对应的迭代器对象
    hi = hashTypeInitIterator(o);
	//循环遍历hash对象中所有的元素
    while (hashTypeNext(hi) != C_ERR) {
		//获取迭代上对应的字段信息
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }
		//获取迭代上对应的值信息
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }
	//释放对应的迭代器空间
    hashTypeReleaseIterator(hi);
    serverAssert(count == length);
}

/*
 * 用于获取哈希表中的所有字段（field）
 * 命令格式
 *     HKEYS key 
 * 返回值
 *     包含哈希表中所有域（field）列表。 当 key 不存在时，返回一个空列表。
 */
void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

/*
 * 返回哈希表所有字段(field)的值
 * 命令格式
 *     HVALS KEY_NAME
 * 返回值
 *     一个包含哈希表中所有域(field)值的列表。 当 key 不存在时，返回一个空表
 */
void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

/*
 * 用于返回哈希表中，所有的字段和值
 *     在返回值里，紧跟每个字段名(field name)之后是字段的值(value)，所以返回值的长度是哈希表大小的两倍
 * 命令格式
 *     HGETALL KEY_NAME
 * 返回值
 *     以列表形式返回哈希表的字段及字段值。 若 key 不存在，返回空列表
 */
void hgetallCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

/*
 * 用于查看哈希表的指定字段是否存在
 * 命令格式
 *     HEXISTS KEY_NAME FIELD_NAME
 * 返回值
 *     如果哈希表含有给定字段，返回 1 。 如果哈希表不含有给定字段，或 key 不存在，返回 0 
 */
void hexistsCommand(client *c) {
    robj *o;
	//检测对应键所对应的值对象是否存在,且是否是hash类型的对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL || checkType(c,o,OBJ_HASH)) 
		return;
	//给客户端响应对应的字段是否存在的标识
    addReply(c, hashTypeExists(o,c->argv[2]->ptr) ? shared.cone : shared.czero);
}

/*
 * 迭代哈希表中的键值对
 *
 */
void hscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) 
		return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL || checkType(c,o,OBJ_HASH)) 
		return;
    scanGenericCommand(c,o,cursor);
}








