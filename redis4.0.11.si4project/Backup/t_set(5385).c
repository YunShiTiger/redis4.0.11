/*
 * redis中集合命令的实现方式
 *    Set 是 String 类型的无序集合。集合成员是唯一的，这就意味着集合中不能出现重复的数据。
 */

#include "server.h"

/*----------------------------------------------------------------------------
 * 集合命令
 * Set Commands
 *----------------------------------------------------------------------------
 */

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum, robj *dstkey, int op);

/* 根据提供的元素的值来确定需要创建何种类型的集合实现方式
 * Factory method to return a set that *can* hold "value". 
 * When the object has an integer-encodable value, an intset will be returned. Otherwise a regular hash table. 
 */
robj *setTypeCreate(sds value) {
    //检测对应的元素是否是长整数数据类型
    if (isSdsRepresentableAsLongLong(value,NULL) == C_OK)
		//创建对应的整数集合类型对象
        return createIntsetObject();
	//创建对应的集合
    return createSetObject();
}

/* 将对应的值插入到集合中
 * Add the specified value into a set.
 *
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. 
 */
int setTypeAdd(robj *subject, sds value) {
    long long llval;
	//根据集合的编码方式不同进行不同的操作处理
    if (subject->encoding == OBJ_ENCODING_HT) {
		//h获取对应的字典结构指向
        dict *ht = subject->ptr;
		//尝试进行元素插入处理
        dictEntry *de = dictAddRaw(ht,value,NULL);
	    //根据返回的元素节点判断是否插入成功
        if (de) {
			//不知道此处为什么由要重新插入一次字段数据的处理
            dictSetKey(ht,de,sdsdup(value));
			//设置一个null值给对应的字段
            dictSetVal(ht,de,NULL);
		    //返回插入数据成功的标识
            return 1;
        }
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        //获取当前需要插入的元素是否是整数数据
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            uint8_t success = 0;
			//将对应的整数元素添加到集合中
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
		    //检测是否插入元素成功
            if (success) {
                /* Convert to regular set when the intset contains too many entries. */
			    //检测整数集合中存储的元素个数是否超过了预设的整数值
                if (intsetLen(subject->ptr) > server.set_max_intset_entries)
					//进行类型转换操作处理
                    setTypeConvert(subject,OBJ_ENCODING_HT);
				//返回添加元素成功操作标识
                return 1;
            }
        } else {
            /* Failed to get integer from object, convert to regular set. */
		    //类型不是整数,直接进行转换类型处理
            setTypeConvert(subject,OBJ_ENCODING_HT);

            /* The set *was* an intset and this value is not integer encodable, so dictAdd should always work. */
			//将对应的元素插入到字典结构中
            serverAssert(dictAdd(subject->ptr,sdsdup(value),NULL) == DICT_OK);
            return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* 将对应的元素在集合中删除处理 */
int setTypeRemove(robj *setobj, sds value) {
    long long llval;
	//根据对象的编码方式进行不同的处理
    if (setobj->encoding == OBJ_ENCODING_HT) {
		//检测是否在字典对象中删除元素成功
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
			//检测是否需要进行调整字典结构的尺寸处理
            if (htNeedsResize(setobj->ptr))
				//进行尺寸变化操作处理
				dictResize(setobj->ptr);
			//返回删除成功标识
            return 1;
        }
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        //检测对应的元素是否是整数类型
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            int success;
			//进行尝试删除操作处理
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
		    //检测是否删除操作成功
            if (success) 
				return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
	//返回对应元素不存在,删除失败的标识
    return 0;
}

/* 检测对应的元素是否存在于集合对象中 */
int setTypeIsMember(robj *subject, sds value) {
    long long llval;
    if (subject->encoding == OBJ_ENCODING_HT) {
		//在字典结构中查询对应的元素是否存在
        return dictFind((dict*)subject->ptr,value) != NULL;
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        //首先检测对应的元素是否是整数类型
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
			//进行查找处理
            return intsetFind((intset*)subject->ptr,llval);
        }
    } else {
        serverPanic("Unknown set encoding");
    }
	//返回没有找到标识
    return 0;
}

/* 根据集合对象来初始化对应的集合迭代器 */
setTypeIterator *setTypeInitIterator(robj *subject) {
    //首先分配迭代器对应的空间
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
	//设置相关参数
    si->subject = subject;
    si->encoding = subject->encoding;
	//根据集合对象的编码类型来获取对应的遍历迭代器
    if (si->encoding == OBJ_ENCODING_HT) {
		//获取字典的迭代器
        si->di = dictGetIterator(subject->ptr);
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        //获取整数集合的迭代位置
        si->ii = 0;
    } else {
        serverPanic("Unknown set encoding");
    }
	//返回对应的迭代器对象指向
    return si;
}

/* 释放对应的迭代器对象占据的空间 */
void setTypeReleaseIterator(setTypeIterator *si) {
    //检测是否是字典类型的
    if (si->encoding == OBJ_ENCODING_HT)
		//释放对应的字典类型迭代器占据的空间
        dictReleaseIterator(si->di);
	//释放集合对象迭代器占据的空间
    zfree(si);
}

/* 获取当前能够遍历到的集合中的元素
 * Move to the next entry in the set. Returns the object at the current position.
 *
 * Since set elements can be internally be stored as SDS strings or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (sdsele) or (llele) accordingly.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When there are no longer elements -1 is returned. 
 */
int setTypeNext(setTypeIterator *si, sds *sdsele, int64_t *llele) {
    //根据遍历类型获取对应的元素位置
    if (si->encoding == OBJ_ENCODING_HT) {
		//在字典结构中获取下一个需要遍历的元素
        dictEntry *de = dictNext(si->di);
		//检测是否找到对应的元素
        if (de == NULL) 
			//返回没有对应元素的标识
			return -1;
		//获取对应的字段内容
        *sdsele = dictGetKey(de);
		//给对应的值内容设置一个默认值---------------------------->其实应该是null的
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
		//在整数集合中获取下一个元素
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
			//返回没有对应元素的标识
            return -1;
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }
	//返回对应的编码方式类型------------->不清楚为什么返回这个值????????????????????????????????????????????
    return si->encoding;
}

/* 根据给定的迭代器中记录的节点元素获取节点元素所对应的一个新的字符串数据
 * The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new SDS
 * strings. So if you don't retain a pointer to this object you should call
 * sdsfree() against it.
 *
 * This function is the way to go for write operations where COW is not an issue. 
 */
sds setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    sds sdsele;
    int encoding;
	//获取对应的下一个元素
    encoding = setTypeNext(si,&sdsele,&intele);
	//根据返回的编码方式来处理获取数据的方式
    switch(encoding) {
        case -1:    
			//返回对应的空对象
			return NULL;
        case OBJ_ENCODING_INTSET:
			//返回对应的整数对应的字符串数据
            return sdsfromlonglong(intele);
        case OBJ_ENCODING_HT:
			//返回新的字符串数据
            return sdsdup(sdsele);
        default:
            serverPanic("Unsupported encoding");
    }
    return NULL; /* just to suppress warnings */
}

/* 在集合对象中随机获取一个元素的值
 * Return random element from a non empty set.
 * The returned element can be a int64_t value if the set is encoded
 * as an "intset" blob of integers, or an SDS string if the set
 * is a regular set.
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointer was populated.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused. */
int setTypeRandomElement(robj *setobj, sds *sdsele, int64_t *llele) {
    //根据当前集合对象的编码方式进行相关处理
    if (setobj->encoding == OBJ_ENCODING_HT) {
		//在字典结构中随机获取一个元素
        dictEntry *de = dictGetRandomKey(setobj->ptr);
		//设置获取的对应元素值--->即对应的字段
        *sdsele = dictGetKey(de);
	    //设置对应的value值------>原始为null对象,此处返回一个默认值了
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
		//在整数集合中获取对应的整数值
        *llele = intsetRandom(setobj->ptr);
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Unknown set encoding");
    }
	//返回当前集合的编码方式-------------->目的是根据返回的编码方式来从传入的两个参数中获取需要获取的元素的值
    return setobj->encoding;
}

/* 获取给定集合对象中元素的个数*/
unsigned long setTypeSize(const robj *subject) {
    //根据当前集合对象的编码方式进行相关处理
    if (subject->encoding == OBJ_ENCODING_HT) {
		//获取字典结构中元素的个数
        return dictSize((const dict*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        //获取整数集合中元素的数量
        return intsetLen((const intset*)subject->ptr);
    } else {
        serverPanic("Unknown set encoding");
    }
}

/* 进行集合底层实现方式的转换处理
 * Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original set. 
 */
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;
    serverAssertWithInfo(NULL,setobj,setobj->type == OBJ_SET && setobj->encoding == OBJ_ENCODING_INTSET);
	//检测是否需要进行转换操作处理
    if (enc == OBJ_ENCODING_HT) {
        int64_t intele;
		//创建对应的字典结构
        dict *d = dictCreate(&setDictType,NULL);
        sds element;

        /* Presize the dict to avoid rehashing */
		//一开始就扩展足够大的空间------>这个方便在后期插入数据时不需要进行扩容操作处理了
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
		//获取一个遍历集合的迭代器对象
        si = setTypeInitIterator(setobj);
		//循环进行迭代将数据插入到新创建的字典结构中
        while (setTypeNext(si,&element,&intele) != -1) {
            element = sdsfromlonglong(intele);
            serverAssert(dictAdd(d,element,NULL) == DICT_OK);
        }
		//释放对应的迭代器对象空间
        setTypeReleaseIterator(si);
		//设置集合对象对应的编码方式
        setobj->encoding = OBJ_ENCODING_HT;
		//释放原始整数集合对应的空间
        zfree(setobj->ptr);
		//设置集合对象内容指向
        setobj->ptr = d;
    } else {
        serverPanic("Unsupported set conversion");
    }
}

/*
 * 将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略
 *     假如集合 key 不存在，则创建一个只包含添加的元素作成员的集合。
 *     当集合 key 不是集合类型时，返回一个错误。 
 * 命令格式
 *     SADD KEY_NAME VALUE1..VALUEN
 * 返回值
 *     被添加到集合中的新元素的数量，不包括被忽略的元素
 */
void saddCommand(client *c) {
    robj *set;
    int j, added = 0;
	//根据给定的键获取对应的值对象
    set = lookupKeyWrite(c->db,c->argv[1]);
	//检测对应的值对象是否存在
    if (set == NULL) {
		//创建对应的集合对象------>这个地方设置的很精巧(通过第一个)
        set = setTypeCreate(c->argv[2]->ptr);
		//将对应的键值对对象插入到redis中
        dbAdd(c->db,c->argv[1],set);
    } else {
		//检测对应的类型是否是集合类型
        if (set->type != OBJ_SET) {
			//返回类型错误响应
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }

    //循环处理插入操作处理
    for (j = 2; j < c->argc; j++) {
		//尝试将对应的元素插入到集合中
        if (setTypeAdd(set,c->argv[j]->ptr)) 
			//设置插入成功元素的计数个数
			added++;
    }
	//检测是否有进行插入元素
    if (added) {
		//发送改变键值对内容的信号
        signalModifiedKey(c->db,c->argv[1]);
		//发送触发对应命令的通知
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }
	//增加脏计数值
    server.dirty += added;
	//返回插入元素的数量值
    addReplyLongLong(c,added);
}

/*
 * 用于移除集合中的一个或多个成员元素，不存在的成员元素会被忽略
 *     当 key 不是集合类型，返回一个错误
 * 命令格式
 *     SREM KEY MEMBER1..MEMBERN
 * 返回值
 *     被成功移除的元素的数量，不包括被忽略的元素
 *
 */
void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    //检测给定的键对象是否存在,且为集合类型
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL || checkType(c,set,OBJ_SET)) 
		return;

    //循环进行删除操作处理
    for (j = 2; j < c->argc; j++) {
		//进行删除对应的元素
        if (setTypeRemove(set,c->argv[j]->ptr)) {
			//进行删除元素计数
            deleted++;
			//检测当前集合中的对象个数是否为0
            if (setTypeSize(set) == 0) {
				//进行删除键值对的操作处理
                dbDelete(c->db,c->argv[1]);
				//设置删除键值对标识
                keyremoved = 1;
			    //跳出循环
                break;
            }
        }
    }
	//检测是否删除元素
    if (deleted) {
		//发送键值对空间变化信号
        signalModifiedKey(c->db,c->argv[1]);
		//发送操作对应命令通知
        notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);
	    //检测是否触发了删除键值对的处理
        if (keyremoved)
			//发送操作对应命令通知
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
		//进行脏数据计数增加
        server.dirty += deleted;
    }
	//向客户端返回删除的元素个数
    addReplyLongLong(c,deleted);
}

/*
 * 将指定成员 member 元素从 source 集合移动到 destination 集合
 *     如果 source 集合不存在或不包含指定的 member 元素，则 SMOVE 命令不执行任何操作，仅返回 0 。否则， member 元素从 source 集合中被移除，并添加到 destination 集合中去
 *     当 destination 集合已经包含 member 元素时， SMOVE 命令只是简单地将 source 集合中的 member 元素删除
 *     当 source 或 destination 不是集合类型时，返回一个错误
 * 命令格式
 *     SMOVE SOURCE DESTINATION MEMBER
 * 返回值
 *     如果成员元素被成功移除，返回 1。 如果成员元素不是 source 集合的成员，并且没有任何操作对 destination 集合执行，那么返回 0
 */
void smoveCommand(client *c) {
    robj *srcset, *dstset, *ele;
	//获取传入的源和目的键所对应的值对象
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    ele = c->argv[3];

    /* If the source key does not exist return 0 */
	//检测源值对象是否存在
    if (srcset == NULL) {
		//向客户端返回0
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key is set and has the wrong type, return with an error. */
    //检测源和目的是否都是集合类型对象
	if (checkType(c,srcset,OBJ_SET) || (dstset && checkType(c,dstset,OBJ_SET))) 
        return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
	//检测给定的源和目的是否是同一个集合对象
    if (srcset == dstset) {
		//根据元素是否存在返回对应的值
        addReply(c,setTypeIsMember(srcset,ele->ptr) ? shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
	//检测元素在源集合对象是是否存在,且进行删除成功
    if (!setTypeRemove(srcset,ele->ptr)) {
        addReply(c,shared.czero);
        return;
    }
	//发送对应的删除命令通知
    notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
	//检测删除元素后,集合元素是否为0
    if (setTypeSize(srcset) == 0) {
		//删除对应的键值对
        dbDelete(c->db,c->argv[1]);
		//发送对应的删除键值对的通知
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Create the destination set when it doesn't exist */
	//检测对应的目的集合对象是否存在
    if (!dstset) {
		//创建对应的集合对象
        dstset = setTypeCreate(ele->ptr);
		//将对应的键值对对象插入到redis中
        dbAdd(c->db,c->argv[2],dstset);
    }
    //发送键值对空间变化信号
    signalModifiedKey(c->db,c->argv[1]);
    signalModifiedKey(c->db,c->argv[2]);
	//增加脏计数值
    server.dirty++;

    /* An extra key has changed when ele was successfully added to dstset */
	//检测是否将元素插入到目的集合对象成功
    if (setTypeAdd(dstset,ele->ptr)) {
		//增加脏计数值
        server.dirty++;
		//发送对应的命令通知
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }
	//向客户端返回元素转移成功标识
    addReply(c,shared.cone);
}

/*
 * 判断成员元素是否是集合的成员
 * 命令格式
 *     SISMEMBER KEY VALUE
 * 返回值
 *     如果成员元素是集合的成员，返回 1 。 如果成员元素不是集合的成员，或 key 不存在，返回 0
 */
void sismemberCommand(client *c) {
    robj *set;
	
	//检测给定的键对象是否存在,且为集合类型
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL || checkType(c,set,OBJ_SET)) 
		return;
	//检测对应的元素是否在集合对象中
    if (setTypeIsMember(set,c->argv[2]->ptr))
		//返回存在标识
        addReply(c,shared.cone);
    else
		//返回不存在标识
        addReply(c,shared.czero);
}

/*
 * 返回集合中元素的数量
 * 命令格式
 *     SCARD KEY_NAME
 * 返回值
 *     集合的数量。 当集合 key 不存在时，返回 0
 */
void scardCommand(client *c) {
    robj *o;
	
	//检测给定的键对象是否存在,且为集合类型
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL || checkType(c,o,OBJ_SET)) 
		return;
	//返回对应集合对象的元素数量
    addReplyLongLong(c,setTypeSize(o));
}

/* Handle the "SPOP key <count>" variant. The normal version of the
 * command is handled by the spopCommand() function itself. */

/* How many times bigger should be the set compared to the remaining size
 * for us to use the "create new set" strategy? Read later in the
 * implementation for more info. */
#define SPOP_MOVE_STRATEGY_MUL 5


void spopWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    robj *set;

    /* Get the count argument */
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) 
		return;
    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        addReply(c,shared.outofrangeerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set. Otherwise, return nil */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL || checkType(c,set,OBJ_SET)) 
        return;

    /* If count is zero, serve an empty multibulk ASAP to avoid special
     * cases later. */
    if (count == 0) {
        addReply(c,shared.emptymultibulk);
        return;
    }

    size = setTypeSize(set);

    /* Generate an SPOP keyspace notification */
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);
    server.dirty += count;

    /* CASE 1:
     * The number of requested elements is greater than or equal to
     * the number of elements inside the set: simply return the whole set. */
    if (count >= size) {
        /* We just return the entire set */
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);

        /* Delete the set as it is now empty */
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);

        /* Propagate this command as an DEL operation */
        rewriteClientCommandVector(c,2,shared.del,c->argv[1]);
        signalModifiedKey(c->db,c->argv[1]);
        server.dirty++;
        return;
    }

    /* Case 2 and 3 require to replicate SPOP as a set of SREM commands.
     * Prepare our replication argument vector. Also send the array length
     * which is common to both the code paths. */
    robj *propargv[3];
    propargv[0] = createStringObject("SREM",4);
    propargv[1] = c->argv[1];
    addReplyMultiBulkLen(c,count);

    /* Common iteration vars. */
    sds sdsele;
    robj *objele;
    int encoding;
    int64_t llele;
    unsigned long remaining = size-count; /* Elements left after SPOP. */

    /* If we are here, the number of requested elements is less than the
     * number of elements inside the set. Also we are sure that count < size.
     * Use two different strategies.
     *
     * CASE 2: The number of elements to return is small compared to the
     * set size. We can just extract random elements and return them to
     * the set. */
    if (remaining*SPOP_MOVE_STRATEGY_MUL > count) {
        while(count--) {
            /* Emit and remove. */
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
                set->ptr = intsetRemove(set->ptr,llele,NULL);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
                setTypeRemove(set,sdsele);
            }

            /* Replicate/AOF this command as an SREM operation */
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
    } else {
    /* CASE 3: The number of elements to return is very big, approaching
     * the size of the set itself. After some time extracting random elements
     * from such a set becomes computationally expensive, so we use
     * a different strategy, we extract random elements that we don't
     * want to return (the elements that will remain part of the set),
     * creating a new set as we do this (that will be stored as the original
     * set). Then we return the elements left in the original set and
     * release it. */
        robj *newset = NULL;

        /* Create a new set with just the remaining elements. */
        while(remaining--) {
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsdup(sdsele);
            }
            if (!newset) newset = setTypeCreate(sdsele);
            setTypeAdd(newset,sdsele);
            setTypeRemove(set,sdsele);
            sdsfree(sdsele);
        }

        /* Assign the new set as the key value. */
        incrRefCount(set); /* Protect the old set value. */
        dbOverwrite(c->db,c->argv[1],newset);

        /* Tranfer the old set to the client and release it. */
        setTypeIterator *si;
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&sdsele,&llele)) != -1) {
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
            }

            /* Replicate/AOF this command as an SREM operation */
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
        setTypeReleaseIterator(si);
        decrRefCount(set);
    }

    /* Don't propagate the command itself even if we incremented the
     * dirty counter. We don't want to propagate an SPOP command since
     * we propagated the command as a set of SREMs operations using
     * the alsoPropagate() API. */
    decrRefCount(propargv[0]);
    preventCommandPropagation(c);
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

/*
 *
 *
 *
 *
 *
 *
 *
 */
void spopCommand(client *c) {
    robj *set, *ele, *aux;
    sds sdsele;
    int64_t llele;
    int encoding;

    if (c->argc == 3) {
        spopWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.nullbulk)) == NULL || checkType(c,set,OBJ_SET)) 
		return;

    /* Get a random element from the set */
    encoding = setTypeRandomElement(set,&sdsele,&llele);

    /* Remove the element from the set */
    if (encoding == OBJ_ENCODING_INTSET) {
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        ele = createStringObject(sdsele,sdslen(sdsele));
        setTypeRemove(set,ele->ptr);
    }

    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */
    aux = createStringObject("SREM",4);
    rewriteClientCommandVector(c,3,aux,c->argv[1],ele);
    decrRefCount(aux);

    /* Add the element to the reply */
    addReplyBulk(c,ele);
    decrRefCount(ele);

    /* Delete the set if it's empty */
    if (setTypeSize(set) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Set has been modified */
    signalModifiedKey(c->db,c->argv[1]);
    server.dirty++;
}

/* 
 * handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. 
 */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. 
 */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

/* 获取指定数量的随机元素 */
void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    int uniq = 1;
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    dict *d;

    //获取对应的数量值
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) 
		return;
	//检测传入的数量值是否大于零
    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        count = -l;
		//设置获取元素可以重复的标识
        uniq = 0;
    }

    //检测给定的键所对应的值对象是否存在,且对应的值对象是否是集合类型
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptymultibulk)) == NULL || checkType(c,set,OBJ_SET)) 
		return;
	//获取集合对象元素个数
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
	//特殊检测传入的数量为0
    if (count == 0) {
		//向客户端返回空
        addReply(c,shared.emptymultibulk);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data structures. */
    //传入参数为负,即允许获取可以重复的元素
    if (!uniq) {
		//设置返回值的空间个数
        addReplyMultiBulkLen(c,count);
		//循环获取元素
        while(count--) {
			//随机获取对应的元素
            encoding = setTypeRandomElement(set,&ele,&llele);
			//根据返回的编码类型获取对应的数据
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c,ele,sdslen(ele));
            }
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    //传入的值大于集合中元素的数量
    if (count >= size) {
		//返回整个集合对象的元素
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    d = dictCreate(&objectKeyPointerValueDictType,NULL);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requsted elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 3 is highly inefficient. */
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            if (encoding == OBJ_ENCODING_INTSET) {
                retval = dictAdd(d,createStringObjectFromLongLong(llele),NULL);
            } else {
                retval = dictAdd(d,createStringObject(ele,sdslen(ele)),NULL);
            }
            serverAssert(retval == DICT_OK);
        }
        setTypeReleaseIterator(si);
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        while(size > count) {
            dictEntry *de;

            de = dictGetRandomKey(d);
            dictDelete(d,dictGetKey(de));
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    else {
        unsigned long added = 0;
        robj *objele;

        while(added < count) {
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                objele = createStringObjectFromLongLong(llele);
            } else {
                objele = createStringObject(ele,sdslen(ele));
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            if (dictAdd(d,objele,NULL) == DICT_OK)
                added++;
            else
                decrRefCount(objele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    {
        dictIterator *di;
        dictEntry *de;

        addReplyMultiBulkLen(c,count);
        di = dictGetIterator(d);
        while((de = dictNext(di)) != NULL)
            addReplyBulk(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

/*
 * 用于返回集合中的一个随机元素
 *     Srandmember 命令接受可选的 count 参数
 *         如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合
 *         如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值
 *     该操作和 SPOP 相似，但 SPOP 将随机元素从集合中移除并返回，而 Srandmember 则仅仅返回随机元素，而不对集合进行任何改动    
 * 命令格式
 *     SRANDMEMBER KEY [count]
 * 返回值
 *     只提供集合 key 参数时，返回一个元素；如果集合为空，返回 nil 。 如果提供了 count 参数，那么返回一个数组；如果集合为空，返回空数组。
 */
void srandmemberCommand(client *c) {
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    //检测传入参数数量是否合法
    if (c->argc == 3) {
		//处理传入参数为3个 即 命令 键 数量值
        srandmemberWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
		//返回参数数量错误的响应
        addReply(c,shared.syntaxerr);
        return;
    }

	//检测给定的键对象是否存在,且为集合类型
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL || checkType(c,set,OBJ_SET)) 
		return;

	//随机获取一个需要的元素
    encoding = setTypeRandomElement(set,&ele,&llele);
	//根据编码方式,获取值的获取方式
    if (encoding == OBJ_ENCODING_INTSET) {
		//返回对应的整数值
        addReplyBulkLongLong(c,llele);
    } else {
        //返回对应的字符串内容
        addReplyBulkCBuffer(c,ele,sdslen(ele));
    }
}

int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    if (setTypeSize(*(robj**)s1) > setTypeSize(*(robj**)s2)) return 1;
    if (setTypeSize(*(robj**)s1) < setTypeSize(*(robj**)s2)) return -1;
    return 0;
}

/* This is used by SDIFF and in this case we can receive NULL that should be handled as empty sets. */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;
    unsigned long first = o1 ? setTypeSize(o1) : 0;
    unsigned long second = o2 ? setTypeSize(o2) : 0;

    if (first < second) return 1;
    if (first > second) return -1;
    return 0;
}

void sinterGenericCommand(client *c, robj **setkeys, unsigned long setnum, robj *dstkey) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds elesds;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding;

    for (j = 0; j < setnum; j++) {
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            zfree(sets);
            if (dstkey) {
                if (dbDelete(c->db,dstkey)) {
                    signalModifiedKey(c->db,dstkey);
                    server.dirty++;
                }
                addReply(c,shared.czero);
            } else {
                addReply(c,shared.emptymultibulk);
            }
            return;
        }
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }
    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    if (!dstkey) {
        replylen = addDeferredMultiBulkLength(c);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        dstset = createIntsetObject();
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    si = setTypeInitIterator(sets[0]);
    while((encoding = setTypeNext(si,&elesds,&intobj)) != -1) {
        for (j = 1; j < setnum; j++) {
            if (sets[j] == sets[0]) continue;
            if (encoding == OBJ_ENCODING_INTSET) {
                /* intset with intset is simple... and fast */
                if (sets[j]->encoding == OBJ_ENCODING_INTSET &&
                    !intsetFind((intset*)sets[j]->ptr,intobj))
                {
                    break;
                /* in order to compare an integer with an object we
                 * have to use the generic function, creating an object
                 * for this */
                } else if (sets[j]->encoding == OBJ_ENCODING_HT) {
                    elesds = sdsfromlonglong(intobj);
                    if (!setTypeIsMember(sets[j],elesds)) {
                        sdsfree(elesds);
                        break;
                    }
                    sdsfree(elesds);
                }
            } else if (encoding == OBJ_ENCODING_HT) {
                if (!setTypeIsMember(sets[j],elesds)) {
                    break;
                }
            }
        }

        /* Only take action when all sets contain the member */
        if (j == setnum) {
            if (!dstkey) {
                if (encoding == OBJ_ENCODING_HT)
                    addReplyBulkCBuffer(c,elesds,sdslen(elesds));
                else
                    addReplyBulkLongLong(c,intobj);
                cardinality++;
            } else {
                if (encoding == OBJ_ENCODING_INTSET) {
                    elesds = sdsfromlonglong(intobj);
                    setTypeAdd(dstset,elesds);
                    sdsfree(elesds);
                } else {
                    setTypeAdd(dstset,elesds);
                }
            }
        }
    }
    setTypeReleaseIterator(si);

    if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        int deleted = dbDelete(c->db,dstkey);
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,"sinterstore",
                dstkey,c->db->id);
        } else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
            if (deleted)
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                    dstkey,c->db->id);
        }
        signalModifiedKey(c->db,dstkey);
        server.dirty++;
    } else {
        setDeferredMultiBulkLength(c,replylen,cardinality);
    }
    zfree(sets);
}


void sinterCommand(client *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

void sinterstoreCommand(client *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}

#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum, robj *dstkey, int op) {
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds ele;
    int j, cardinality = 0;
    int diff_algo = 1;

    for (j = 0; j < setnum; j++) {
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    if (op == SET_OP_DIFF && sets[0]) {
        long long algo_one_work = 0, algo_two_work = 0;

        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            algo_one_work += setTypeSize(sets[0]);
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            qsort(sets+1,setnum-1,sizeof(robj*),qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    dstset = createIntsetObject();

    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (setTypeAdd(dstset,ele)) 
					cardinality++;
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);
        }
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        si = setTypeInitIterator(sets[0]);
        while((ele = setTypeNextObject(si)) != NULL) {
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) 
					continue; /* no key is an empty set. */
                if (sets[j] == sets[0]) 
					break; /* same set! */
                if (setTypeIsMember(sets[j],ele)) 
					break;
            }
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                setTypeAdd(dstset,ele);
                cardinality++;
            }
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) 
				continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (j == 0) {
                    if (setTypeAdd(dstset,ele)) cardinality++;
                } else {
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            if (cardinality == 0) 
				break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    if (!dstkey) {
        addReplyMultiBulkLen(c,cardinality);
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL) {
            addReplyBulkCBuffer(c,ele,sdslen(ele));
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
        decrRefCount(dstset);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        int deleted = dbDelete(c->db,dstkey);
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,
                op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
        } else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
            if (deleted)
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
        }
        signalModifiedKey(c->db,dstkey);
        server.dirty++;
    }
    zfree(sets);
}

void sunionCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_UNION);
}

void sunionstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_UNION);
}

void sdiffCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_DIFF);
}

void sdiffstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_DIFF);
}

void sscanCommand(client *c) {
    robj *set;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) 
		return;
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL || checkType(c,set,OBJ_SET)) 
		return;
    scanGenericCommand(c,set,cursor);
}
















