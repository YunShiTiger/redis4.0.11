/*
 * Redis中字符串对象的实现操作处理
 */

#include "server.h"
#include <math.h> /* isnan(), isinf() */

/*-----------------------------------------------------------------------------
 * 字符串类型对象提供的相关命令
 * String Commands
 *----------------------------------------------------------------------------
 */

/* 用于检测对应的字符串对象的长度是否超过了redis中预设的值 512M*/
static int checkStringLength(client *c, long long size) {
    if (size > 512*1024*1024) {
        addReplyError(c,"string exceeds maximum allowed size (512MB)");
        return C_ERR;
    }
    return C_OK;
}

/* 
 * The setGenericCommand() function implements the SET operation with different
 * options and variants. This function is called in order to implement the
 * following commands: SET, SETEX, PSETEX, SETNX.
 *
 * 'flags' changes the behavior of the command (NX or XX, see belove).
 *
 * 'expire' represents an expire to set in form of a Redis object as passed
 * by the user. It is interpreted according to the specified 'unit'.
 *
 * 'ok_reply' and 'abort_reply' is what the function will reply to the client
 * if the operation is performed, or when it is not because of NX or
 * XX flags.
 *
 * If ok_reply is NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used. 
 */

#define OBJ_SET_NO_FLAGS 0
#define OBJ_SET_NX (1<<0)     /* Set if key not exists. 在key不存在的情况下才会设置 */
#define OBJ_SET_XX (1<<1)     /* Set if key exists. 在key存在的情况下才会设置 */
#define OBJ_SET_EX (1<<2)     /* Set if time in seconds is given 以秒(s)为单位设置键的key过期时间 */
#define OBJ_SET_PX (1<<3)     /* Set if time in ms in given 以毫秒(ms)为单位设置键的key过期时间 */

/* setGenericCommand()函数是以下命令: SET, SETEX, PSETEX, SETNX.的最底层实现
 * flags 可以是NX或XX，由上面的宏提供
 * expire 定义key的过期时间，格式由unit指定
 * ok_reply和abort_reply保存着回复client的内容，NX和XX也会改变回复
 * 如果ok_reply为空，则使用 "+OK"
 * 如果abort_reply为空，则使用 "$-1"
 */
void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    /* initialized to avoid any harmness warning */
    long long milliseconds = 0; 

    //检测是否设置了过期时间参数对象
    if (expire) {
		//获取设置的过期时间对象是否是合法参数,并获取对应的过期时间值
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != C_OK)
            return;
		//检测设置的过期时间值是否合法,非负值
        if (milliseconds <= 0) {
            addReplyErrorFormat(c,"invalid expire time in %s",c->cmd->name);
            return;
        }
		//计算对应的毫秒值
        if (unit == UNIT_SECONDS) 
			milliseconds *= 1000;
    }

    //根据对应的标识和是否已经存在了对应的键值对对象来向客户端进行响应
    if ((flags & OBJ_SET_NX && lookupKeyWrite(c->db,key) != NULL) || (flags & OBJ_SET_XX && lookupKeyWrite(c->db,key) == NULL)) {
		//说明已经存在了对应的对象,不能进行设置操作处理
        addReply(c, abort_reply ? abort_reply : shared.nullbulk);
        return;
    }
	//此处是真正的核心,用于将对应的字符串数据设置到键值对对象上--------->核心操作
    setKey(c->db,key,val);
	//增加脏数据计数值
    server.dirty++;
	//检测是否设置了过期时间
    if (expire) 
		//给对应的键值对设置过期时间值
		setExpire(c,c->db,key,mstime()+milliseconds);
	//发送触发相关命令的通知处理
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);
    if (expire) 
		//发送触发相关命令的通知处理
		notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);
	//向客户端返回操作成功的响应结果
    addReply(c, ok_reply ? ok_reply : shared.ok);
}

/* 
 * 用于设置给定 key 的值。如果 key 已经存储其他值， SET 就覆写旧值，且无视类型。
 * 命令格式
 *     SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>]
 * 返回值
 *     在 Redis 2.6.12 以前版本，SET 命令总是返回 OK 
 *     从 Redis 2.6.12 版本开始，SET 在设置操作成功完成时，才返回 OK 
 */
void setCommand(client *c) {
    int j;
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
	//初始化标记
    int flags = OBJ_SET_NO_FLAGS;

    //循环遍历客户端传入的后续参数
    for (j = 3; j < c->argc; j++) {
        char *a = c->argv[j]->ptr;
		//用于记录是否有下一个参数值
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];
        if ((a[0] == 'n' || a[0] == 'N') && (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && !(flags & OBJ_SET_XX)) {
			//设置NX标记  
            flags |= OBJ_SET_NX;
        } else if ((a[0] == 'x' || a[0] == 'X') && (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && !(flags & OBJ_SET_NX)) {
            //设置XX标记  
            flags |= OBJ_SET_XX;
        } else if ((a[0] == 'e' || a[0] == 'E') && (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && !(flags & OBJ_SET_PX) && next) {
			//设置EX标记  
            flags |= OBJ_SET_EX;
			//设置时间类型
            unit = UNIT_SECONDS;
			//记录设置的过期时间对象
            expire = next;
			//注意此处需要进行跳过对应的参数
            j++;
        } else if ((a[0] == 'p' || a[0] == 'P') && (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' && !(flags & OBJ_SET_EX) && next) {
			//设置PX标记  
            flags |= OBJ_SET_PX;
			//设置时间类型
            unit = UNIT_MILLISECONDS;
			//记录设置的过期时间对象
            expire = next;
			//注意此处需要进行跳过对应的参数
            j++;
        } else {
			//向客户端返回传入参数格式出现问题的错误响应
            addReply(c,shared.syntaxerr);
            return;
        }
    }
	
	//对value进行最优的编码
    c->argv[2] = tryObjectEncoding(c->argv[2]);
	//触发通用的进行设置字符串对象的处理
    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

/* 
 * Redis Setnx（SET if Not eXists） 命令在指定的 key 不存在时，为 key 设置指定的值
 * 命令格式
 *     SETNX KEY_NAME VALUE
 * 返回值
 *     设置成功，返回 1 。 设置失败，返回 0 。
 */
void setnxCommand(client *c) {
    //对value进行最优的编码
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,OBJ_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

/* 
 * 为指定的 key 设置值及其过期时间。如果 key 已经存在， SETEX 命令将会替换旧的值
 * 命令格式
 *     SETEX KEY_NAME TIMEOUT VALUE
 * 返回值
 *     置成功时返回 OK 
 */
void setexCommand(client *c) {
	//对value进行最优的编码
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

/* 
 * 以毫秒为单位设置 key 的生存时间
 * 命令格式
 *     PSETEX KEY_NAME EXPIRY_IN_MILLISECONDS value1
 * 返回值
 *     设置成功时返回 OK
 */
void psetexCommand(client *c) {
	//对value进行最优的编码
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

/* 通用的用于获取对应的值的处理函数 */
int getGenericCommand(client *c) {
    robj *o;
	//首先检测对应的键所对应的值对象是否存在
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) == NULL)
        return C_OK;
	//检测对应的值对象是否是字符串类型对象
    if (o->type != OBJ_STRING) {
		//向客户端返回对应的类型不匹配的错误响应
        addReply(c,shared.wrongtypeerr);
        return C_ERR;
    } else {
		//向客户端返回对应的值对象的响应
        addReplyBulk(c,o);
        return C_OK;
    }
}

/* 
 * 用于获取指定 key 的值。如果 key 不存在，返回 nil 。如果key 储存的值不是字符串类型，返回一个错误
 * 命令格式
 *     GET KEY_NAME
 * 返回值
 *     返回 key 的值，如果 key 不存在时，返回 nil。 如果 key 不是字符串类型，那么返回一个错误
 */
void getCommand(client *c) {
    getGenericCommand(c);
}

/* 
 * 用于设置指定 key 的值，并返回 key 的旧值
 * 命令格式
 *     GETSET KEY_NAME VALUE
 * 返回值
 *     返回给定 key 的旧值。 当 key 没有旧值时，即 key 不存在时，返回 nil 
 *     当 key 存在但不是字符串类型时，返回一个错误
 */
void getsetCommand(client *c) {
    //首先尝试向客户端返回对应键的原始值对象
    if (getGenericCommand(c) == C_ERR) 
		return;
    c->argv[2] = tryObjectEncoding(c->argv[2]);
	//将新的值对象设置到redis中
    setKey(c->db,c->argv[1],c->argv[2]);
	//发送触发相关命令的通知
    notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[1],c->db->id);
	//增加对应的脏数据计数
    server.dirty++;
}

/* 
 * 用指定的字符串覆盖给定 key 所储存的字符串值，覆盖的位置从偏移量 offset 开始
 * 命令格式
 *     SETRANGE KEY_NAME OFFSET VALUE
 * 返回值
 *     被修改后的字符串长度
 */
void setrangeCommand(client *c) {
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr;

	//首先获取对应的偏移量位置值
    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != C_OK)
        return;

    //检测偏移量是否合法
    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    //获取对应键所对应的值对象
    o = lookupKeyWrite(c->db,c->argv[1]);
	//检测对应的值对象是否存在
    if (o == NULL) {
        /* Return 0 when setting nothing on a non-existing string */
	    //检测传入的字符串对象长度是否为0
        if (sdslen(value) == 0) {
			//不进行相关命令处理---->同时向客户端返回0值
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
		//检测进行增长处理时字符串的长度是否超过了预设的最大长度值
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;
		//创建对应大小的字符串对象------>即开辟了对应大小的空间
        o = createObject(OBJ_STRING,sdsnewlen(NULL, offset+sdslen(value)));
		//将对应的键值对添加到redis中
        dbAdd(c->db,c->argv[1],o);
    } else {
        size_t olen;

        /* Key exists, check type */
		//检测对应的值对象是否是字符串对象
        if (checkType(c,o,OBJ_STRING))
            return;

        /* Return existing string length when setting nothing */
		//获取老对象的字符串长度
        olen = stringObjectLen(o);
		//检测老对象字符串长度是否为0
        if (sdslen(value) == 0) {
			//直接向客户端返回老对象的长度0.同时不进行后续操作处理
            addReplyLongLong(c,olen);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
		//检测进行增长处理时字符串的长度是否超过了预设的最大长度值
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        /* Create a copy when the object is shared or encoded. */
		//处理字符串对象因为共享问题而出现的拷贝问题------------------->即字符串对象被多个地方共享,不能随便在此处进行修改对象,不然所有引用处都发送了变化
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

	//检测传入的字符串对象长度是否非0
    if (sdslen(value) > 0) {
		//给对应的字符串指向位置开辟足够大的空间,同时在指定位置开始,后续内容全是0
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));
		//将新的字符串内容拷贝到对应的空间中
        memcpy((char*)o->ptr+offset,value,sdslen(value));
	    //发送键值对空间变化的信号
        signalModifiedKey(c->db,c->argv[1]);
		//发送触发对应命令的通知
        notifyKeyspaceEvent(NOTIFY_STRING,"setrange",c->argv[1],c->db->id);
		//增加脏计数值
        server.dirty++;
    }
	//向客户端返回操作之后字符串对象的长度值
    addReplyLongLong(c,sdslen(o->ptr));
}

/* 
 * 用于获取存储在指定 key 中字符串的子字符串。字符串的截取范围由 start 和 end 两个偏移量决定(包括 start 和 end 在内)
 * 命令格式
 *     GETRANGE KEY_NAME start end
 * 返回值
 *     截取得到的子字符串
 */
void getrangeCommand(client *c) {
    robj *o;
    long long start, end;
    char *str, llbuf[32];
    size_t strlen;

    //获取截取起始点
    if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
        return;
	//获取截取结束点
    if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
        return;
	//检测对应键对象是否存在,且对应的对象是否是字符串类型对象
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL || checkType(c,o,OBJ_STRING)) 
		return;

    //检测对应的字符串对象的编码方式是否是整数类型
    if (o->encoding == OBJ_ENCODING_INT) {
        str = llbuf;
		//将对应的整数类型转换成对应的字符串形式
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
		//获取对应的字符串数据指向位置
        str = o->ptr;
        strlen = sdslen(str);
    }

    /* Convert negative indexes */
	//处理起始和结束位置全为负值,且范围不对的错误
    if (start < 0 && end < 0 && start > end) {
        addReply(c,shared.emptybulk);
        return;
    }
	//处理负值问题
    if (start < 0) 
		start = strlen+start;
    if (end < 0) 
		end = strlen+end;
	
	//处理设置负值大于字符串长度的问题
    if (start < 0) 
		start = 0;
    if (end < 0) 
		end = 0;
	
	//设置结束位置大于字符串长度问题
    if ((unsigned long long)end >= strlen) 
		end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where nothing can be returned is: start > end. */
	//进一步检测范围是否合法
    if (start > end || strlen == 0) {
		//向客户端返回空对象
        addReply(c,shared.emptybulk);
    } else {
        //向客户端返回指定长度的字符串内容
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

/* 
 * 返回所有(一个或多个)给定 key 的值。 如果给定的 key 里面，有某个 key 不存在，那么这个 key 返回特殊值 nil
 * 命令格式
 *      MGET KEY1 KEY2 .. KEYN
 * 返回值
 *     一个包含所有给定 key 的值的列表
 */
void mgetCommand(client *c) {
    int j;
	//设定需要返回客户端的空间个数大小
    addReplyMultiBulkLen(c,c->argc-1);
	//循环获取对应的字符串对象
    for (j = 1; j < c->argc; j++) {
		//获取对应键所对应的值对象
        robj *o = lookupKeyRead(c->db,c->argv[j]);
		//检测对应的值对象是否存在
        if (o == NULL) {
			//设置对应的空对象
            addReply(c,shared.nullbulk);
        } else {
            //检测对应的类型是否是字符串类型
            if (o->type != OBJ_STRING) {
				//类型错误设置空对象------------------>注意这个地方没有发送类型错误的响应处理
                addReply(c,shared.nullbulk);
            } else {
                //设置对应的字符串对象
                addReplyBulk(c,o);
            }
        }
    }
}

/* 处理一次设置多个字符串对象的通用操作处理 */
void msetGenericCommand(client *c, int nx) {
    int j, busykeys = 0;

    //检测对应的参数个数是否有问题---->即需要成对出现
    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }
    /* Handle the NX flag. The MSETNX semantic is to return zero and don't set nothing at all if at least one already key exists. */
    if (nx) {
		//循环检测对应的键对象是否已经存在
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                busykeys++;
            }
        }
		//检测给定的多个键是否有已经存在的了
        if (busykeys) {
			//直接向客户端返回设置失败的标识响应
            addReply(c, shared.czero);
            return;
        }
    }

    //循环进行设置新键值对的操作处理
    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
		//设置对应的新的键值对空间值
        setKey(c->db,c->argv[j],c->argv[j+1]);
	    //发送触发对应命令的通知
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id);
    }
	//增加脏数据计数
    server.dirty += (c->argc-1)/2;
	//向客户端发送对应的响应结果
    addReply(c, nx ? shared.cone : shared.ok);
}

/* 
 * 用于同时设置一个或多个 key-value 对
 * 命令格式
 *     MSET key1 value1 key2 value2 .. keyN valueN
 * 返回值
 *     总是返回 OK
 */
void msetCommand(client *c) {
    msetGenericCommand(c,0);
}

/* 
 * 用于所有给定 key 都不存在时，同时设置一个或多个 key-value 对
 * 命令格式
 *     MSETNX key1 value1 key2 value2 .. keyN valueN
 * 返回值
 *     当所有 key 都成功设置，返回 1 。 如果所有给定 key 都设置失败(至少有一个 key 已经存在)则不进行设置操作，同时返回 0
 */
void msetnxCommand(client *c) {
    msetGenericCommand(c,1);
}

/* 通用的进行对字符串对象值进行增减操作 */
void incrDecrCommand(client *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    //获取对应键所对应的值对象
    o = lookupKeyWrite(c->db,c->argv[1]);
	//检测对应的值对象是否存在,且是否是字符串类型对象
    if (o != NULL && checkType(c,o,OBJ_STRING)) 
		return;
	//获取值对象所对应的整数值
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != C_OK) 
		return;

    //用于记录老值
    oldvalue = value;
	//检测增量后范围是否越界
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) || (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }

	//增加增量值
    value += incr;

    //检测对应的整数字符串对象是否是共享类型
    if (o && o->refcount == 1 && o->encoding == OBJ_ENCODING_INT && (value < 0 || value >= OBJ_SHARED_INTEGERS) && value >= LONG_MIN && value <= LONG_MAX) {
        
		new = o;
        o->ptr = (void*)((long)value);
    } else {
        new = createStringObjectFromLongLong(value);
        if (o) {
            dbOverwrite(c->db,c->argv[1],new);
        } else {
            dbAdd(c->db,c->argv[1],new);
        }
    }
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrby",c->argv[1],c->db->id);
    server.dirty++;
    addReply(c,shared.colon);
    addReply(c,new);
    addReply(c,shared.crlf);
}

/*
 * 将 key 中储存的数字值增一
 *      如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作
 *      如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误
 *      本操作的值限制在 64 位(bit)有符号数字表示之内
 * 命令格式
 *     INCR KEY_NAME
 * 返回值
 *     执行命令之后 key 的值
 */
void incrCommand(client *c) {
    incrDecrCommand(c,1);
}

/*
 * 将 key 中储存的数字值减一
 *      如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作
 *      如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误
 *      本操作的值限制在 64 位(bit)有符号数字表示之内
 * 命令格式
 *     DECR KEY_NAME
 * 返回值
 *     执行命令之后 key 的值
 */
void decrCommand(client *c) {
    incrDecrCommand(c,-1);
}

/*
 * 将 key 中储存的数字加上指定的增量值
 *      如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作
 *      如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误
 *      本操作的值限制在 64 位(bit)有符号数字表示之内
 * 命令格式
 *     INCRBYFLOAT KEY_NAME INCR_AMOUNT
 * 返回值
 *     加上指定的增量值之后， key 的值
 */
void incrbyCommand(client *c) {
    long long incr;
	
	//获取对应的增量值
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) 
		return;
    incrDecrCommand(c,incr);
}

/*
 * 将 key 所储存的值减去指定的减量值
 *      如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作
 *      如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误
 *      本操作的值限制在 64 位(bit)有符号数字表示之内
 * 命令格式
 *     DECRBY KEY_NAME DECREMENT_AMOUNT
 * 返回值
 *     减去指定减量值之后，key的值
 */
void decrbyCommand(client *c) {
    long long incr;
	
	//获取对应的增量值
    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) 
		return;
    incrDecrCommand(c,-incr);
}

void incrbyfloatCommand(client *c) {
    long double incr, value;
    robj *o, *new, *aux;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o != NULL && checkType(c,o,OBJ_STRING)) 
		return;
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != C_OK || getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != C_OK)
        return;

    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }
    new = createStringObjectFromLongDouble(value,1);
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);
    server.dirty++;
    addReplyBulk(c,new);

    /* Always replicate INCRBYFLOAT as a SET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    aux = createStringObject("SET",3);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,2,new);
}

void appendCommand(client *c) {
    size_t totlen;
    robj *o, *append;

    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Create the key */
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        /* Key exists, check type */
        if (checkType(c,o,OBJ_STRING))
            return;

        /* "append" is an argument, so always an sds */
        append = c->argv[2];
        totlen = stringObjectLen(o)+sdslen(append->ptr);
        if (checkStringLength(c,totlen) != C_OK)
            return;

        /* Append the value */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }
    signalModifiedKey(c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"append",c->argv[1],c->db->id);
    server.dirty++;
    addReplyLongLong(c,totlen);
}

void strlenCommand(client *c) {
    robj *o;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;
    addReplyLongLong(c,stringObjectLen(o));
}








