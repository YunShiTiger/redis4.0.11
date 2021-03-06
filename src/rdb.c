/*
 * redis中rdb备份的实现方式
 */

#include "server.h"
#include "lzf.h"    /* LZF compression library */
#include "zipmap.h"
#include "endianconv.h"

#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/param.h>

#define rdbExitReportCorruptRDB(...) rdbCheckThenExit(__LINE__,__VA_ARGS__)

extern int rdbCheckMode;
void rdbCheckError(const char *fmt, ...);
void rdbCheckSetError(const char *fmt, ...);

//检查rdb错误发送信息且退出
void rdbCheckThenExit(int linenum, char *reason, ...) {
    va_list ap;
    char msg[1024];
    int len;
	//将错误信息写到msg缓冲区中
    len = snprintf(msg, sizeof(msg), "Internal error in RDB reading function at rdb.c:%d -> ", linenum);
    //用reason初始化ap
	va_start(ap,reason);
	//将ap指向的参数写到len长度的后面
    vsnprintf(msg+len,sizeof(msg)-len,reason,ap);
	//结束关闭
    va_end(ap);

    //发送错误信息
    if (!rdbCheckMode) {
        serverLog(LL_WARNING, "%s", msg);
        char *argv[2] = {"",server.rdb_filename};
        redis_check_rdb_main(2,argv,NULL);
    } else {
        rdbCheckError("%s",msg);
    }
	//退出程序
    exit(1);
}

/* 将长度为len的数组p写到rdb中，返回写的长度 最为原始的进行数据写入的处理函数 */
static int rdbWriteRaw(rio *rdb, void *p, size_t len) {
    if (rdb && rioWrite(rdb,p,len) == 0)
        return -1;
    return len;
}

/* 将长度为1字节的type字符写到rdb中 主要是写入对象的编码类型或者对应的过期时间标识 */
int rdbSaveType(rio *rdb, unsigned char type) {
    //调用原始的写入一个字符的处理函数
    return rdbWriteRaw(rdb,&type,1);
}

/* 从rdb中载入1字节的数据保存在type中，并返回其type
 * 这里的type类型 分为几种
 *        辅助标识------->键值对
 *        库索引标识----->库索引
 *        过期时间标识--->过期时间值
 *        结束标记------->键值对内容读取结束
 *        调整哈希表标识-->设置hash表的大小值
 * Load a "type" in RDB format, that is a one byte unsigned integer.
 * This function is not only used to load object types, but also special
 * "types" like the end-of-file type, the EXPIRE type, and so forth. 
 */
int rdbLoadType(rio *rdb) {
    unsigned char type;
	//读取一个字节的type类型
    if (rioRead(rdb,&type,1) == 0) 
		return -1;
    return type;
}

//从rio读出一个时间，单位为秒，长度为4字节
time_t rdbLoadTime(rio *rdb) {
    int32_t t32;
	//读取4个字节的时间值
    if (rioRead(rdb,&t32,4) == 0) 
		return -1;
    return (time_t)t32;
}

//写一个longlong类型的时间，单位为毫秒
int rdbSaveMillisecondTime(rio *rdb, long long t) {
    int64_t t64 = (int64_t) t;
	//写入一个8字节的毫秒时间值
    return rdbWriteRaw(rdb,&t64,8);
}

//从rio中读出一个毫秒时间返回
long long rdbLoadMillisecondTime(rio *rdb) {
    int64_t t64;
	//从rio中读出一个8字节的毫秒时间值
    if (rioRead(rdb,&t64,8) == 0) 
		return -1;
    return (long long)t64;
}

/* 将一个被编码的长度写入到rio中，返回保存编码后的len需要的字节数
 * 此处需要分4中情况进行分别处理
 *       00 + 6位长度值               1字节
 *       01 + 14位长度值              2字节
 *       1000 0000 + 32位长度值       5字节
 *       1000 0001 + 64位长度值       9字节
 * 通过第一个字节编码方式可以知道 00 01 10 三种编码方式开头 标识后续是一个整数值
 * 同时表明后续有整数值个字符串的数据
 * Saves an encoded length. The first two bits in the first byte are used to
 * hold the encoding type. See the RDB_* definitions for more information
 * on the types of encoding. 
 */
int rdbSaveLen(rio *rdb, uint64_t len) {
    unsigned char buf[2];
    size_t nwritten;

    if (len < (1<<6)) {
		//长度小于2^6
		//高两位是00表示6位长，第六位表示len的值
        buf[0] = (len&0xFF)|(RDB_6BITLEN<<6);
		//将buf[0]写到rio中
        if (rdbWriteRaw(rdb,buf,1) == -1) 
			return -1;
		//返回1字节
        nwritten = 1;
    } else if (len < (1<<14)) {
        //长度小于2^14
        //高两位是01表示14位长，剩下的14位表示len的值
        buf[0] = ((len>>8)&0xFF)|(RDB_14BITLEN<<6);
        buf[1] = len&0xFF;
		//将buf[0..1]写到rio中
        if (rdbWriteRaw(rdb,buf,2) == -1) 
			return -1;
		//返回2字节
        nwritten = 2;
    } else if (len <= UINT32_MAX) {
        //长度大于UINT32_MAX
        //高两位为10表示32位长，剩下的6位不使用
        buf[0] = RDB_32BITLEN;
		//将buf[0]写入
        if (rdbWriteRaw(rdb,buf,1) == -1) 
			return -1;
		//将len转换为网络序，写入rdb中
        uint32_t len32 = htonl(len);
		//存储对应的真实长度值
        if (rdbWriteRaw(rdb,&len32,4) == -1) 
			return -1;
		//返回5个字节
        nwritten = 1+4;
    } else {
        //长度大于2^14
        //高两位为10 最低位01 中间全是0 表示64位长
        buf[0] = RDB_64BITLEN;
		//将buf[0]写入
        if (rdbWriteRaw(rdb,buf,1) == -1) 
			return -1;
		//将len转换为网络序，写入rdb中
        len = htonu64(len);
		//存储对应的真实长度值
        if (rdbWriteRaw(rdb,&len,8) == -1) 
			return -1;
		//返回9个字节
        nwritten = 1+8;
    }
	//返回写入整数数据占据的字节数量
    return nwritten;
}


/* 一个从rio读出的len值，如果该len值不是整数，而是被编码后的值，那么将isencoded设置为1
 * Load an encoded length. If the loaded length is a normal length as stored
 * with rdbSaveLen(), the read length is set to '*lenptr'. If instead the
 * loaded length describes a special encoding that follows, then '*isencoded'
 * is set to 1 and the encoding format is stored at '*lenptr'.
 *
 * See the RDB_ENC_* definitions in rdb.h for more information on special encodings.
 *
 * The function returns -1 on error, 0 on success. 
 */
int rdbLoadLenByRef(rio *rdb, int *isencoded, uint64_t *lenptr) {
    unsigned char buf[2];
    int type;

    //默认为没有编码
    if (isencoded) 
		*isencoded = 0;
	//将rio中的值读到buf中
    if (rioRead(rdb,buf,1) == 0) 
		return -1;
	//(buf[0]&0xC0)>>6 = (1100 000 & buf[0]) >> 6 = buf[0]的最高两位
    type = (buf[0]&0xC0)>>6;
	//根据获取的高位类型来获取长度数据
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) 
			//一个编码过的值，返回解码值，设置编码标志
			*isencoded = 1;
		//取出剩下六位表示的长度值
        *lenptr = buf[0]&0x3F;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
	    //取出剩下六位表示的长度值
        *lenptr = buf[0]&0x3F;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
	    //从buf+1读出1个字节的值
        if (rioRead(rdb,buf+1,1) == 0) 
			return -1;
		//取出除最高两位的长度值
        *lenptr = ((buf[0]&0x3F)<<8)|buf[1];
    } else if (buf[0] == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        uint32_t len;
		//读出4个字节的值
        if (rioRead(rdb,&len,4) == 0) 
			return -1;
		//转换为主机序的值
        *lenptr = ntohl(len);
    } else if (buf[0] == RDB_64BITLEN) {
        /* Read a 64 bit len. */
        uint64_t len;
		//读出8个字节的值
        if (rioRead(rdb,&len,8) == 0) 
			return -1;
		//转换为主机序的值
        *lenptr = ntohu64(len);
    } else {
        rdbExitReportCorruptRDB("Unknown length encoding %d in rdbLoadLen()",type);
        return -1; /* Never reached. */
    }
    return 0;
}

/* 返回一个从rio读出的len值，如果该len值不是整数，而是被编码后的值，那么将isencoded设置为1
 * This is like rdbLoadLenByRef() but directly returns the value read
 * from the RDB stream, signaling an error by returning RDB_LENERR
 * (since it is a too large count to be applicable in any Redis data structure). 
 */
uint64_t rdbLoadLen(rio *rdb, int *isencoded) {
    uint64_t len;
	//获取对应的长度值
    if (rdbLoadLenByRef(rdb,isencoded,&len) == -1) 
		return RDB_LENERR;
    return len;
}

/* 将longlong类型的value编码成一个整数编码，如果可以编码，将编码后的值保存在enc中，返回编码后的字节数
 * 数据的格式 第一个字节表示编码整数的类型 后面的字节用于存储数据
 * 初次存储整数数据有3中形式
 *         1100 0000 + 8位整数值
 *         1100 0001 + 16位整数值
 *         1100 0010 + 32位整数值
 * Encodes the "value" argument as integer when it fits in the supported ranges
 * for encoded types. If the function successfully encodes the integer, the
 * representation is stored in the buffer pointer to by "enc" and the string
 * length is returned. Otherwise 0 is returned. 
 */
int rdbEncodeInteger(long long value, unsigned char *enc) {
    if (value >= -(1<<7) && value <= (1<<7)-1) {
		//-2^8 <= value <= 2^8-1
        //最高两位设置为 11，表示是一个编码过的值，低6位为 000000 ，表示是 RDB_ENC_INT8 编码格式
        //剩下的1个字节保存value，返回2字节
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT8;
        enc[1] = value&0xFF;
        return 2;
    } else if (value >= -(1<<15) && value <= (1<<15)-1) {
		//-2^16 <= value <= 2^16-1
        //最高两位设置为 11，表示是一个编码过的值，低6位为 000001 ，表示是 RDB_ENC_INT16 编码格式
        //剩下的2个字节保存value，返回3字节
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT16;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        return 3;
    } else if (value >= -((long long)1<<31) && value <= ((long long)1<<31)-1) {
		//-2^32 <= value <= 2^32-1
        //最高两位设置为 11，表示是一个编码过的值，低6位为 000010 ，表示是 RDB_ENC_INT32 编码格式
        //剩下的4个字节保存value，返回5字节
        enc[0] = (RDB_ENCVAL<<6)|RDB_ENC_INT32;
        enc[1] = value&0xFF;
        enc[2] = (value>>8)&0xFF;
        enc[3] = (value>>16)&0xFF;
        enc[4] = (value>>24)&0xFF;
        return 5;
    } else {
        return 0;
    }
}

/* 
 * Loads an integer-encoded object with the specified encoding type "enctype".
 * The returned value changes according to the flags, see rdbGenerincLoadStringObject() for more info. 
 */
void *rdbLoadIntegerObject(rio *rdb, int enctype, int flags, size_t *lenptr) {
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    int encode = flags & RDB_LOAD_ENC;
    unsigned char enc[4];
    long long val;

    if (enctype == RDB_ENC_INT8) {
        if (rioRead(rdb,enc,1) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb,enc,2) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb,enc,4) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        rdbExitReportCorruptRDB("Unknown RDB integer encoding type %d",enctype);
    }
    if (plain || sds) {
        char buf[LONG_STR_SIZE], *p;
        int len = ll2string(buf,sizeof(buf),val);
        if (lenptr) *lenptr = len;
        p = plain ? zmalloc(len) : sdsnewlen(NULL,len);
        memcpy(p,buf,len);
        return p;
    } else if (encode) {
        return createStringObjectFromLongLong(val);
    } else {
        return createObject(OBJ_STRING,sdsfromlonglong(val));
    }
}

/* 将一些纯数字的字符串尝试转换为可以编码的整数，以节省磁盘空间
 * String objects in the form "2391" "-100" without any space and with a
 * range of values that can fit in an 8, 16 or 32 bit signed value can be
 * encoded as integers to save space 
 */
int rdbTryIntegerEncoding(char *s, size_t len, unsigned char *enc) {
    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
	//尝试将字符串值转换为整数
    value = strtoll(s, &endptr, 10);
	//字符串不是纯数字的返回0
    if (endptr[0] != '\0') 
		return 0;
	//将value转回字符串类型
    ll2string(buf,32,value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    //比较转换前后的字符串，如果不相等，则返回0
    if (strlen(buf) != len || memcmp(buf,s,len)) 
		return 0;
	
	//可以编码则转换成整数，将编码类型保存enc中
    return rdbEncodeInteger(value,enc);
}

/*  将讲一个LZF压缩过的字符串的信息写入rio，返回写入的字节数 */
ssize_t rdbSaveLzfBlob(rio *rdb, void *data, size_t compress_len, size_t original_len) {
    unsigned char byte;
    ssize_t n, nwritten = 0;

    /* Data compressed! Let's save it on disk */
	//将1100 0011保存在byte中，表示编码过，是一个LZF压缩的字符串
    byte = (RDB_ENCVAL<<6)|RDB_ENC_LZF;
	//将byte写入rio中
    if ((n = rdbWriteRaw(rdb,&byte,1)) == -1) 
		goto writeerr;
    nwritten += n;
	//将压缩后的长度值compress_len写入rio
    if ((n = rdbSaveLen(rdb,compress_len)) == -1) 
		goto writeerr;
    nwritten += n;
	//将压缩前的长度值original_len写入rio
    if ((n = rdbSaveLen(rdb,original_len)) == -1) 
		goto writeerr;
    nwritten += n;
	//将压缩的字符串值data写入rio
    if ((n = rdbWriteRaw(rdb,data,compress_len)) == -1) 
		goto writeerr;
    nwritten += n;
	//返回写入的字节数
    return nwritten;

writeerr:
    return -1;
}

/* 将对应的字符串数据进行编码压缩处理,并进行存储操作 */
ssize_t rdbSaveLzfStringObject(rio *rdb, unsigned char *s, size_t len) {
    size_t comprlen, outlen;
    void *out;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= 4) 
		return 0;
    outlen = len-4;
	//开辟对应大小的空间
    if ((out = zmalloc(outlen+1)) == NULL) 
		return 0;
	//进行字符串数据压缩操作处理
    comprlen = lzf_compress(s, len, out, outlen);
	//检测是否压缩数据成功
    if (comprlen == 0) {
		//释放对应的空间
        zfree(out);
		//返回进行压缩操作处理失败的标识
        return 0;
    }
	//将压缩后的数据进行存储操作处理
    ssize_t nwritten = rdbSaveLzfBlob(rdb, out, comprlen, len);
	//释放对应的空间
    zfree(out);
	//返回占据的空间字节数量
    return nwritten;
}

/* Load an LZF compressed string in RDB format. The returned value
 * changes according to 'flags'. For more info check the
 * rdbGenericLoadStringObject() function. */
void *rdbLoadLzfStringObject(rio *rdb, int flags, size_t *lenptr) {
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    uint64_t len, clen;
    unsigned char *c = NULL;
    char *val = NULL;

    if ((clen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;
    if ((c = zmalloc(clen)) == NULL) goto err;

    /* Allocate our target according to the uncompressed size. */
    if (plain) {
        val = zmalloc(len);
        if (lenptr) *lenptr = len;
    } else {
        val = sdsnewlen(NULL,len);
    }

    /* Load the compressed representation and uncompress it to target. */
    if (rioRead(rdb,c,clen) == 0) goto err;
    if (lzf_decompress(c,clen,val,len) == 0) {
        if (rdbCheckMode) rdbCheckSetError("Invalid LZF compressed string");
        goto err;
    }
    zfree(c);

    if (plain || sds) {
        return val;
    } else {
        return createObject(OBJ_STRING,val);
    }
err:
    zfree(c);
    if (plain)
        zfree(val);
    else
        sdsfree(val);
    return NULL;
}

/* 将一个原生的字符串值写入到rio 
 * 此处分三种情况进行处理
 *     1 能够转换成整数的  进行存储对应的整数数据---->第一个字节表示整数类型 后续字节中存储真正的数据
 *     2 长度大于20的进行压缩操作处理 然后进行存储
 *     3 中等短字符串直接进行存储处理
 * 进一步对存储的字符串(整数类型的字符串)数据进行分析可以获得以下经验值
 *
 *    真正的整数数据
 *          1100 0000 + 8位整数值
 *          1100 0001 + 16位整数值
 *          1100 0010 + 32位整数值
 *    真正的字符串数据
 *          1100 0011 + 压缩后整数数据长度值 + 压缩前整数数据长度值 + 压缩的字符串真实数据内容
 *          
 *          00        + 6位长度值           + 字符串的数据内容
 *          01        + 14位长度值          + 字符串的数据内容
 *          1000 0000 + 32位长度值          + 字符串的数据内容
 *          1000 0001 + 64位长度值          + 字符串的数据内容
 *
 *  所以通过第一个字节的内容来检测拿去的数据时何种数据
 *
 * Save a string object as [len][data] on disk. If the object is a string
 * representation of an integer value we try to save it in a special form 
 */
ssize_t rdbSaveRawString(rio *rdb, unsigned char *s, size_t len) {
    int enclen;
    ssize_t n, nwritten = 0;

    /* Try integer encoding */
	//如果字符串可以进行整数编码
    if (len <= 11) {
        unsigned char buf[5];
		//尝试进行整数编码操作处理
        if ((enclen = rdbTryIntegerEncoding((char*)s,len,buf)) > 0) {
			//将编码后的字符串写入rio，返回编码所需的字节数
            if (rdbWriteRaw(rdb,buf,enclen) == -1) 
				return -1;
			//返回写入数据占据的字节数量
            return enclen;
        }
    }

    /* 
     * Try LZF compression - under 20 bytes it's unable to compress even aaaaaaaaaaaaaaaaaa so skip it 
     */
    //如果开启了rdb压缩的设置，且字符串长度大于20，进行LZF压缩字符串
    if (server.rdb_compression && len > 20) {
		//进行压缩存储操作处理
        n = rdbSaveLzfStringObject(rdb,s,len);
        if (n == -1) 
			return -1;
        if (n > 0) 
			return n;
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
	//字符串既不能被压缩，也不能编码成整数 因此直接写入rio中
    //首先写入对应的长度值 
    if ((n = rdbSaveLen(rdb,len)) == -1) 
		return -1;
    nwritten += n;
    if (len > 0) {
		//然后写入字符串数据
        if (rdbWriteRaw(rdb,s,len) == -1) 
			return -1;
        nwritten += len;
    }
	//返回写入本次数据占据的空间数量
    return nwritten;
}

/* 将 longlong类型的value转换为字符串对象，并且进行编码，然后写到rio中
 * Save a long long value as either an encoded string or a string. 
 */
ssize_t rdbSaveLongLongAsStringObject(rio *rdb, long long value) {
    unsigned char buf[32];
    ssize_t n, nwritten = 0;
	//将longlong类型value进行整数编码并将值写到buf中，节约内存----->即拼接成对应的字节数据
    int enclen = rdbEncodeInteger(value,buf);
	//如果可以进行整数编码
    if (enclen > 0) {
		//将整数编码后的longlong值写到rio中
        return rdbWriteRaw(rdb,buf,enclen);
    } else {
        //不能进行整数编码,转换为字符串
        enclen = ll2string((char*)buf,32,value);
        serverAssert(enclen < 32);
		//将字符串长度写入rio中
        if ((n = rdbSaveLen(rdb,enclen)) == -1) 
			return -1;
        nwritten += n;
		//将字符串写入rio中
        if ((n = rdbWriteRaw(rdb,buf,enclen)) == -1) 
			return -1;
        nwritten += n;
    }
	//发送写入的长度
    return nwritten;
}

/* 将字符串对象obj写到rio中
 * Like rdbSaveRawString() gets a Redis object instead. 
 */
ssize_t rdbSaveStringObject(rio *rdb, robj *obj) {
    /* Avoid to decode the object, then encode it again, if the object is already integer encoded. */
    //检测是否是int编码字符串对象
    if (obj->encoding == OBJ_ENCODING_INT) {
		//将对象值进行编码后发送给rio
        return rdbSaveLongLongAsStringObject(rdb,(long)obj->ptr);
    } else {
        serverAssertWithInfo(NULL,obj,sdsEncodedObject(obj));
		//RAW或EMBSTR编码类型的字符串对象,将字符串类型的对象写到rio
        return rdbSaveRawString(rdb,obj->ptr,sdslen(obj->ptr));
    }
}

/* 根据flags，将从rio读出一个字符串对象进行编码
 * RDB_LOAD_NONE：读一个rio，不编码
 * Load a string object from an RDB file according to flags:
 *
 * RDB_LOAD_NONE (no flags): load an RDB object, unencoded.
 * RDB_LOAD_ENC: If the returned type is a Redis object, try to
 *               encode it in a special way to be more memory
 *               efficient. When this flag is passed the function
 *               no longer guarantees that obj->ptr is an SDS string.
 * RDB_LOAD_PLAIN: Return a plain string allocated with zmalloc()
 *                 instead of a Redis object with an sds in it.
 * RDB_LOAD_SDS: Return an SDS string instead of a Redis object.
 *
 * On I/O error NULL is returned.
 */
void *rdbGenericLoadStringObject(rio *rdb, int flags, size_t *lenptr) {
    //解析对应的状态标识
    //编码
    int encode = flags & RDB_LOAD_ENC;
	//原生的值
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    int isencoded;
    uint64_t len;

    //从rio中读出一个字符串对象，编码类型保存在isencoded中，所需的字节为len
    len = rdbLoadLen(rdb,&isencoded);
	//如果读出的对象被编码(isencoded被设置为1)，则根据不同的长度值len映射到不同的整数编码
    if (isencoded) {
        switch(len) {
        case RDB_ENC_INT8:
        case RDB_ENC_INT16:
        case RDB_ENC_INT32:
			//以上三种类型的整数编码，根据flags返回不同类型值
            return rdbLoadIntegerObject(rdb,len,flags,lenptr);
        case RDB_ENC_LZF:
			//如果是压缩后的字符串，进行构建压缩字符串编码对象
            return rdbLoadLzfStringObject(rdb,flags,lenptr);
        default:
            rdbExitReportCorruptRDB("Unknown RDB string encoding type %d",len);
        }
    }

    //如果len值错误，则返回NULL
    if (len == RDB_LENERR) 
		return NULL;
	
    if (plain || sds) {
        void *buf = plain ? zmalloc(len) : sdsnewlen(NULL,len);
        if (lenptr) *lenptr = len;
        if (len && rioRead(rdb,buf,len) == 0) {
            if (plain)
                zfree(buf);
            else
                sdsfree(buf);
            return NULL;
        }
        return buf;
    } else {
        robj *o = encode ? createStringObject(NULL,len) : createRawStringObject(NULL,len);
        if (len && rioRead(rdb,o->ptr,len) == 0) {
            decrRefCount(o);
            return NULL;
        }
        return o;
    }
}

//从rio中读出一个字符串编码的对象
robj *rdbLoadStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,RDB_LOAD_NONE,NULL);
}

robj *rdbLoadEncodedStringObject(rio *rdb) {
    return rdbGenericLoadStringObject(rdb,RDB_LOAD_ENC,NULL);
}

/* Save a double value. Doubles are saved as strings prefixed by an unsigned
 * 8 bit integer specifying the length of the representation.
 * This 8 bit integer has special values in order to specify the following
 * conditions:
 * 253: not a number
 * 254: + inf
 * 255: - inf
 */
int rdbSaveDoubleValue(rio *rdb, double val) {
    unsigned char buf[128];
    int len;

    if (isnan(val)) {
        buf[0] = 253;
        len = 1;
    } else if (!isfinite(val)) {
        len = 1;
        buf[0] = (val < 0) ? 255 : 254;
    } else {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
        /* Check if the float is in a safe range to be casted into a
         * long long. We are assuming that long long is 64 bit here.
         * Also we are assuming that there are no implementations around where
         * double has precision < 52 bit.
         *
         * Under this assumptions we test if a double is inside an interval
         * where casting to long long is safe. Then using two castings we
         * make sure the decimal part is zero. If all this is true we use
         * integer printing function that is much faster. */
        double min = -4503599627370495; /* (2^52)-1 */
        double max = 4503599627370496; /* -(2^52) */
        if (val > min && val < max && val == ((double)((long long)val)))
            ll2string((char*)buf+1,sizeof(buf)-1,(long long)val);
        else
#endif
            snprintf((char*)buf+1,sizeof(buf)-1,"%.17g",val);
        buf[0] = strlen((char*)buf+1);
        len = buf[0]+1;
    }
    return rdbWriteRaw(rdb,buf,len);
}

/* For information about double serialization check rdbSaveDoubleValue() */
int rdbLoadDoubleValue(rio *rdb, double *val) {
    char buf[256];
    unsigned char len;

    if (rioRead(rdb,&len,1) == 0) return -1;
    switch(len) {
    case 255: *val = R_NegInf; return 0;
    case 254: *val = R_PosInf; return 0;
    case 253: *val = R_Nan; return 0;
    default:
        if (rioRead(rdb,buf,len) == 0) return -1;
        buf[len] = '\0';
        sscanf(buf, "%lg", val);
        return 0;
    }
}

/* Saves a double for RDB 8 or greater, where IE754 binary64 format is assumed.
 * We just make sure the integer is always stored in little endian, otherwise
 * the value is copied verbatim from memory to disk.
 *
 * Return -1 on error, the size of the serialized value on success. */
int rdbSaveBinaryDoubleValue(rio *rdb, double val) {
    memrev64ifbe(&val);
    return rdbWriteRaw(rdb,&val,sizeof(val));
}

/* Loads a double from RDB 8 or greater. See rdbSaveBinaryDoubleValue() for
 * more info. On error -1 is returned, otherwise 0. */
int rdbLoadBinaryDoubleValue(rio *rdb, double *val) {
    if (rioRead(rdb,val,sizeof(*val)) == 0) return -1;
    memrev64ifbe(val);
    return 0;
}

/* Like rdbSaveBinaryDoubleValue() but single precision. */
int rdbSaveBinaryFloatValue(rio *rdb, float val) {
    memrev32ifbe(&val);
    return rdbWriteRaw(rdb,&val,sizeof(val));
}

/* Like rdbLoadBinaryDoubleValue() but single precision. */
int rdbLoadBinaryFloatValue(rio *rdb, float *val) {
    if (rioRead(rdb,val,sizeof(*val)) == 0) return -1;
    memrev32ifbe(val);
    return 0;
}

/* 将对象o的类型写到rio中
 * Save the object type of object "o". 
 */
int rdbSaveObjectType(rio *rdb, robj *o) {
    //根据不同数据类型，写入不同编码类型
    switch (o->type) {
		//字符串类型
    	case OBJ_STRING:
        	return rdbSaveType(rdb,RDB_TYPE_STRING);
	   //列表类型
    	case OBJ_LIST:
        	if (o->encoding == OBJ_ENCODING_QUICKLIST)
            	return rdbSaveType(rdb,RDB_TYPE_LIST_QUICKLIST);
        	else
            	serverPanic("Unknown list encoding");
		//集合类型
    	case OBJ_SET:
        	if (o->encoding == OBJ_ENCODING_INTSET)
            	return rdbSaveType(rdb,RDB_TYPE_SET_INTSET);
        	else if (o->encoding == OBJ_ENCODING_HT)
            	return rdbSaveType(rdb,RDB_TYPE_SET);
        	else
            	serverPanic("Unknown set encoding");
		//有序集合类型
    	case OBJ_ZSET:
        	if (o->encoding == OBJ_ENCODING_ZIPLIST)
            	return rdbSaveType(rdb,RDB_TYPE_ZSET_ZIPLIST);
        	else if (o->encoding == OBJ_ENCODING_SKIPLIST)
            	return rdbSaveType(rdb,RDB_TYPE_ZSET_2);
        	else
            	serverPanic("Unknown sorted set encoding");
		//哈希类型
    	case OBJ_HASH:
        	if (o->encoding == OBJ_ENCODING_ZIPLIST)
            	return rdbSaveType(rdb,RDB_TYPE_HASH_ZIPLIST);
        	else if (o->encoding == OBJ_ENCODING_HT)
            	return rdbSaveType(rdb,RDB_TYPE_HASH);
        	else
            	serverPanic("Unknown hash encoding");
		//模块类型
    	case OBJ_MODULE:
        	return rdbSaveType(rdb,RDB_TYPE_MODULE_2);
    	default:
        	serverPanic("Unknown object type");
    	}
    return -1; /* avoid warning */
}

/* 
 * Use rdbLoadType() to load a TYPE in RDB format, but returns -1 if the
 * type is not specifically a valid Object Type. 
 */
int rdbLoadObjectType(rio *rdb) {
    int type;
    if ((type = rdbLoadType(rdb)) == -1) 
		return -1;
    if (!rdbIsObjectType(type)) 
		return -1;
    return type;
}

/* 将一个对象写到rio中，出错返回-1，成功返回写的字节数
 * Save a Redis object. Returns -1 on error, number of bytes written on success. 
 */
ssize_t rdbSaveObject(rio *rdb, robj *o) {
    ssize_t n = 0, nwritten = 0;
	//根据对象类型和编码方式不同进行不同方式的存储处理
    if (o->type == OBJ_STRING) {
        //保存字符串对象
        if ((n = rdbSaveStringObject(rdb,o)) == -1) 
			return -1;
        nwritten += n;
    } else if (o->type == OBJ_LIST) {
        //保存一个列表对象
        //列表对象编码为quicklist
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
			//表头地址
            quicklist *ql = o->ptr;
			//头节点地址
            quicklistNode *node = ql->head;
		    //将quicklist的节点个数写入rio中
            if ((n = rdbSaveLen(rdb,ql->len)) == -1) 
				return -1;
            nwritten += n;
			//循环操作处理所有的节点数据
            while(node) {
				//检测当前节点是否可以进行压缩操作处理
                if (quicklistNodeIsCompressed(node)) {
                    void *data;
					//将节点的数据压缩到data中
                    size_t compress_len = quicklistGetLzf(node, &data);
				    //将一个压缩过的字符串写到rio中
                    if ((n = rdbSaveLzfBlob(rdb,data,compress_len,node->sz)) == -1) 
						return -1;
                    nwritten += n;
                } else {
					//将一个原生的字符串写入到rio中
                    if ((n = rdbSaveRawString(rdb,node->zl,node->sz)) == -1) 
						return -1;
                    nwritten += n;
                }
				//设置下一个需要遍历的节点
                node = node->next;
            }
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        //保存一个集合对象
        if (o->encoding == OBJ_ENCODING_HT) {
			//获取字典指向
            dict *set = o->ptr;
			//获取对应的迭代器
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;
			//首先写入元素的数量
            if ((n = rdbSaveLen(rdb,dictSize(set))) == -1) 
				return -1;
            nwritten += n;
			//遍历集合成员
            while((de = dictNext(di)) != NULL) {
				//获取对应的字段值
                sds ele = dictGetKey(de);
				//将当前节点保存的键对象写入rio中
                if ((n = rdbSaveRawString(rdb,(unsigned char*)ele,sdslen(ele)))== -1) 
                    return -1;
                nwritten += n;
            }
			//释放字典迭代器
            dictReleaseIterator(di);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
			//获取intset所占的字节数
            size_t l = intsetBlobLen((intset*)o->ptr);
			//以一个原生字符串对象的方式将intset集合写到rio中
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) 
				return -1;
            nwritten += n;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        //保存一个有序集合对象
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
			//有序集合对象是ziplist类型
			//获取ziplist所占的字节数
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);
			//以一个原生字符串对象保存ziplist类型的有序集合
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) 
				return -1;
            nwritten += n;
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
			//有序集合对象是skiplist类型
            zset *zs = o->ptr;
			//获取对应的跳跃表指向
            zskiplist *zsl = zs->zsl;

			//将有序集合的节点个数写入rio中
            if ((n = rdbSaveLen(rdb,zsl->length)) == -1) 
				return -1;
            nwritten += n;

            /* We save the skiplist elements from the greatest to the smallest
             * (that's trivial since the elements are already ordered in the
             * skiplist): this improves the load process, since the next loaded
             * element will always be the smaller, so adding to the skiplist
             * will always immediately stop at the head, making the insertion
             * O(1) instead of O(log(N)). 
             */
            //获取对应的尾部节点指向
            zskiplistNode *zn = zsl->tail;
			//遍历所有节点
            while (zn != NULL) {
				//以字符串对象的形式将键对象写到rio中
                if ((n = rdbSaveRawString(rdb, (unsigned char*)zn->ele,sdslen(zn->ele))) == -1) {
                    return -1;
                }
                nwritten += n;
				//将double值转换为字符串对象，写到rio中
                if ((n = rdbSaveBinaryDoubleValue(rdb,zn->score)) == -1)
                    return -1;
                nwritten += n;
				//向前进行遍历节点
                zn = zn->backward;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        //保存一个哈希对象
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
			//哈希对象是ziplist类型的
			//ziplist所占的字节数
            size_t l = ziplistBlobLen((unsigned char*)o->ptr);
			//以一个原生字符串对象保存ziplist类型的有序集合
            if ((n = rdbSaveRawString(rdb,o->ptr,l)) == -1) 
				return -1;
            nwritten += n;
        } else if (o->encoding == OBJ_ENCODING_HT) {
			//创建字典迭代器
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            //将哈希表的节点个数写入rio中
            if ((n = rdbSaveLen(rdb,dictSize((dict*)o->ptr))) == -1) 
				return -1;
            nwritten += n;
			
			//遍历整个哈希表
            while((de = dictNext(di)) != NULL) {
				//获取当前节点保存的键
                sds field = dictGetKey(de);
				//键对应的值
                sds value = dictGetVal(de);
			    //以字符串对象的形式将键对象和值对象写到rio中
                if ((n = rdbSaveRawString(rdb,(unsigned char*)field,sdslen(field))) == -1) 
					return -1;
                nwritten += n;
                if ((n = rdbSaveRawString(rdb,(unsigned char*)value,sdslen(value))) == -1) 
					return -1;
                nwritten += n;
            }
			//释放对应的迭代器
            dictReleaseIterator(di);
        } else {
            serverPanic("Unknown hash encoding");
        }

    } else if (o->type == OBJ_MODULE) {
        /* Save a module-specific value. */
        RedisModuleIO io;
        moduleValue *mv = o->ptr;
        moduleType *mt = mv->type;
        moduleInitIOContext(io,mt,rdb);

        /* Write the "module" identifier as prefix, so that we'll be able
         * to call the right module during loading. */
        int retval = rdbSaveLen(rdb,mt->id);
        if (retval == -1) 
			return -1;
        io.bytes += retval;

        /* Then write the module-specific representation + EOF marker. */
        mt->rdb_save(&io,mv->value);
        retval = rdbSaveLen(rdb,RDB_MODULE_OPCODE_EOF);
        if (retval == -1) 
			return -1;
        io.bytes += retval;

        if (io.ctx) {
            moduleFreeContext(io.ctx);
            zfree(io.ctx);
        }
        return io.error ? -1 : (ssize_t)io.bytes;
    } else {
        serverPanic("Unknown object type");
    }
	//返回写入的字节数量
    return nwritten;
}

/* Return the length the object will have on disk if saved with
 * the rdbSaveObject() function. Currently we use a trick to get
 * this length with very little changes to the code. In the future
 * we could switch to a faster solution. 
 */
//返回一个对象的长度，通过写入的方式
//但是已经被放弃使用
size_t rdbSavedObjectLen(robj *o) {
    ssize_t len = rdbSaveObject(NULL,o);
    serverAssertWithInfo(NULL,o,len != -1);
    return len;
}

/* 将一个键对象，值对象，过期时间，和类型写入到rio中，出错返回-1，成功返回1，键过期返回0
 * 通过对源码的分析可以知道如何经验
 *       1 首先根据第一个字节中的数据来确定 是过期时间类型 还是值对象的编码类型
 *             如果是过期时间类型           后续的内容将时对应的过期时间值
 *             如果是对象编码类型           后续的内容就是对应的键对象和值对象
 * Save a key-value pair, with expire time, type, key, value.
 * On error -1 is returned.
 * On success if the key was actually saved 1 is returned, otherwise 0
 * is returned (the key was already expired). 
 */
int rdbSaveKeyValuePair(rio *rdb, robj *key, robj *val, long long expiretime) {
    /* Save the expire time */
    //保存过期时间
    if (expiretime != -1) {
		//将一个毫秒的过期时间类型写入rio
        if (rdbSaveType(rdb,RDB_OPCODE_EXPIRETIME_MS) == -1) 
			return -1;
		//将过期时间写入rio
        if (rdbSaveMillisecondTime(rdb,expiretime) == -1) 
			return -1;
    }

    /* Save type, key, value */
	//将值对象的类型写入到rio中
    if (rdbSaveObjectType(rdb,val) == -1) 
		return -1;
	//将键对象的类型写入到rio中
    if (rdbSaveStringObject(rdb,key) == -1) 
		return -1;
	//将值对象的类型写入到rio中--------------------->此处是核心写入的处理,即如何写入对应的值对象
    if (rdbSaveObject(rdb,val) == -1) 
		return -1;
	//写入完成返回成功标识
    return 1;
}

/* 写入一个特殊的辅助操作码字段
 * Save an AUX field. 
 */
ssize_t rdbSaveAuxField(rio *rdb, void *key, size_t keylen, void *val, size_t vallen) {
    ssize_t ret, len = 0;
	//RDB_OPCODE_AUX 对应的操作码是250
    if ((ret = rdbSaveType(rdb,RDB_OPCODE_AUX)) == -1) 
		return -1;
    len += ret;
	//写入键对象
    if ((ret = rdbSaveRawString(rdb,key,keylen)) == -1) 
		return -1;
    len += ret;
	//写入值对象
    if ((ret = rdbSaveRawString(rdb,val,vallen)) == -1) 
		return -1;
    len += ret;
	//返回总共写入的元素占据的空间数量
    return len;
}

/* rdbSaveAuxField()的封装，适用于key和val是c语言字符串类型
 * Wrapper for rdbSaveAuxField() used when key/val length can be obtained with strlen(). 
 */
ssize_t rdbSaveAuxFieldStrStr(rio *rdb, char *key, char *val) {
    return rdbSaveAuxField(rdb,key,strlen(key),val,strlen(val));
}

/* rdbSaveAuxField()的封装，适用于key是c语言类型字符串，val是一个longlong类型的整数
 * 处理思路:先将对应的整数转换成对应的字符串数据,在后续存储本字符串数据时在转换成对应的整数存储
 * 这样的好处,统一使用一个函数进行处理,在真正进行数据存储时,在进行区分存储
 * Wrapper for strlen(key) + integer type (up to long long range). 
 */
ssize_t rdbSaveAuxFieldStrInt(rio *rdb, char *key, long long val) {
    char buf[LONG_STR_SIZE];
    int vlen = ll2string(buf,sizeof(buf),val);
    return rdbSaveAuxField(rdb,key,strlen(key),buf,vlen);
}

/* 将一个rdb文件的默认信息写入到rio中
 * Save a few default AUX fields with information about the RDB generated. 
 */
int rdbSaveInfoAuxFields(rio *rdb, int flags, rdbSaveInfo *rsi) {
    //判断主机的总线宽度，是64位还是32位
    int redis_bits = (sizeof(void*) == 8) ? 64 : 32;
    int aof_preamble = (flags & RDB_SAVE_AOF_PREAMBLE) != 0;

    /* Add a few fields about the state when the RDB was created. */
	//添加rdb文件的状态信息：Redis版本，redis位数，当前时间和Redis当前使用的内存数
    if (rdbSaveAuxFieldStrStr(rdb,"redis-ver",REDIS_VERSION) == -1) 
		return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"redis-bits",redis_bits) == -1) 
		return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"ctime",time(NULL)) == -1) 
		return -1;
    if (rdbSaveAuxFieldStrInt(rdb,"used-mem",zmalloc_used_memory()) == -1) 
		return -1;

    /* Handle saving options that generate aux fields. */
    if (rsi) {
        if (rdbSaveAuxFieldStrInt(rdb,"repl-stream-db",rsi->repl_stream_db) == -1) 
			return -1;
        if (rdbSaveAuxFieldStrStr(rdb,"repl-id",server.replid) == -1) 
			return -1;
        if (rdbSaveAuxFieldStrInt(rdb,"repl-offset",server.master_repl_offset) == -1) 
			return -1;
    }
    if (rdbSaveAuxFieldStrInt(rdb,"aof-preamble",aof_preamble) == -1) 
		return -1;
    return 1;
}

/* 将一个RDB格式文件内容写入到rio中，成功返回C_OK，否则C_ERR和一部分或所有的出错信息
 * 当函数返回C_ERR，并且error不是NULL，那么error被设置为一个错误码errno
 * Produces a dump of the database in RDB format sending it to the specified
 * Redis I/O channel. On success C_OK is returned, otherwise C_ERR
 * is returned and part of the output, or all the output, can be
 * missing because of I/O errors.
 *
 * When the function returns C_ERR and if 'error' is not NULL, the
 * integer pointed by 'error' is set to the value of errno just after the I/O error. 
 */
int rdbSaveRio(rio *rdb, int *error, int flags, rdbSaveInfo *rsi) {
    dictIterator *di = NULL;
    dictEntry *de;
    char magic[10];
    int j;
    uint64_t cksum;
    size_t processed = 0;

    //检测是否开启了校验和选项
    if (server.rdb_checksum)
		//设置校验和的函数
        rdb->update_cksum = rioGenericUpdateChecksum;
	//将Redis版本信息保存到magic中
    snprintf(magic,sizeof(magic),"REDIS%04d",RDB_VERSION);
	//将magic写到rio中
    if (rdbWriteRaw(rdb,magic,9) == -1) 
		goto werr;
	//将rdb文件的默认信息写到rio中
    if (rdbSaveInfoAuxFields(rdb,flags,rsi) == -1) 
		goto werr;
	//遍历redis中所有库中的数据,进行数据备份操作处理
    for (j = 0; j < server.dbnum; j++) {
		//获取当前索引对应的库
        redisDb *db = server.db+j;
		//获取当前库对应的数据集
        dict *d = db->dict;
	    //检测当前库中是否有数据需要存储处理
        if (dictSize(d) == 0) 
			continue;
		//获取对应的迭代器对象
        di = dictGetSafeIterator(d);
		//检测获取对应的迭代器是否成功
        if (!di) 
			return C_ERR;

        /* Write the SELECT DB opcode */
		//写入数据库的选择标识码 RDB_OPCODE_SELECTDB为254
        if (rdbSaveType(rdb,RDB_OPCODE_SELECTDB) == -1) 
			goto werr;
		//写入数据库的id，占了一个字节的长度
        if (rdbSaveLen(rdb,j) == -1) 
			goto werr;

        /* 写入调整数据库的操作码，我们将大小限制在UINT32_MAX以内，这并不代表数据库的实际大小，只是提示去重新调整哈希表的大小
         * Write the RESIZE DB opcode. We trim the size to UINT32_MAX, which
         * is currently the largest type we are able to represent in RDB sizes.
         * However this does not limit the actual size of the DB to load since
         * these sizes are just hints to resize the hash tables. 
         */
        uint32_t db_size, expires_size;
		//如果字典的大小大于UINT32_MAX，则设置db_size为最大的UINT32_MAX
        db_size = (dictSize(db->dict) <= UINT32_MAX) ? dictSize(db->dict) : UINT32_MAX;
		//设置有过期时间键的大小超过UINT32_MAX，则设置expires_size为最大的UINT32_MAX
        expires_size = (dictSize(db->expires) <= UINT32_MAX) ? dictSize(db->expires) : UINT32_MAX;
		//写入调整哈希表大小的操作码，RDB_OPCODE_RESIZEDB = 251
        if (rdbSaveType(rdb,RDB_OPCODE_RESIZEDB) == -1) 
			goto werr;
		//写入提示调整哈希表大小的值
        if (rdbSaveLen(rdb,db_size) == -1) 
			goto werr;
		//写入提示调整哈希表大小的值
        if (rdbSaveLen(rdb,expires_size) == -1) 
			goto werr;

        /* Iterate this DB writing every entry */
		//遍历数据库所有的键值对
        while((de = dictNext(di)) != NULL) {
			//当前键字符串
            sds keystr = dictGetKey(de);
			//当前键对应的值对象
            robj key, *o = dictGetVal(de);
            long long expire;
			//在栈中创建一个键对象并初始化
            initStaticStringObject(key,keystr);
			//获取当前键的过期时间
            expire = getExpire(db,&key);
			//将键的键对象，值对象，过期时间写到rio中
            if (rdbSaveKeyValuePair(rdb,&key,o,expire) == -1) 
				goto werr;

            /* 
             * When this RDB is produced as part of an AOF rewrite, move
             * accumulated diff from parent to child while rewriting in
             * order to have a smaller final write. 
             */
            if (flags & RDB_SAVE_AOF_PREAMBLE && rdb->processed_bytes > processed+AOF_READ_DIFF_INTERVAL_BYTES) {
                processed = rdb->processed_bytes;
                aofReadDiffFromParent();
            }
        }
		//释放迭代器
        dictReleaseIterator(di);
    }
    di = NULL; /* So that we don't release it again on error. */

    /* If we are storing the replication information on disk, persist
     * the script cache as well: on successful PSYNC after a restart, we need
     * to be able to process any EVALSHA inside the replication backlog the master will send us. 
     */
    //处理lua脚本的操作
    if (rsi && dictSize(server.lua_scripts)) {
		//获取对应的迭代器
        di = dictGetIterator(server.lua_scripts);
		//循环遍历所有的脚本数据
        while((de = dictNext(di)) != NULL) {
			//获取对应的脚本内容
            robj *body = dictGetVal(de);
			//将对应的脚本存储到rio中
            if (rdbSaveAuxField(rdb,"lua",3,body->ptr,sdslen(body->ptr)) == -1)
                goto werr;
        }
		//释放对应的迭代器
        dictReleaseIterator(di);
    }

    /* EOF opcode */
	//数据库数据设置完成之后,写入一个结束标记符
    if (rdbSaveType(rdb,RDB_OPCODE_EOF) == -1)
		goto werr;

    /* CRC64 checksum. It will be zero if checksum computation is disabled, the loading code skips the check in this case. */
    //获取当前rdb中记录的校验码值
    cksum = rdb->cksum;
	//进行校验码值处理
    memrev64ifbe(&cksum);
	//将8位校验码写入到文件的最后
    if (rioWrite(rdb,&cksum,8) == 0) 
		goto werr;
    return C_OK;

werr:
    if (error) 
		//设置存储失败原因
		*error = errno;
	//检测对应的迭代器是否存在
    if (di) 
		//释放对应的空间
		dictReleaseIterator(di);
    return C_ERR;
}

/* This is just a wrapper to rdbSaveRio() that additionally adds a prefix
 * and a suffix to the generated RDB dump. The prefix is:
 *
 * $EOF:<40 bytes unguessable hex string>\r\n
 *
 * While the suffix is the 40 bytes hex string we announced in the prefix.
 * This way processes receiving the payload can understand when it ends
 * without doing any processing of the content. */
int rdbSaveRioWithEOFMark(rio *rdb, int *error, rdbSaveInfo *rsi) {
    char eofmark[RDB_EOF_MARK_SIZE];

    getRandomHexChars(eofmark,RDB_EOF_MARK_SIZE);
    if (error) 
		*error = 0;
    if (rioWrite(rdb,"$EOF:",5) == 0) 
		goto werr;
    if (rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) 
		goto werr;
    if (rioWrite(rdb,"\r\n",2) == 0) goto werr;
    if (rdbSaveRio(rdb,error,RDB_SAVE_NONE,rsi) == C_ERR) 
		goto werr;
    if (rioWrite(rdb,eofmark,RDB_EOF_MARK_SIZE) == 0) 
		goto werr;
    return C_OK;

werr: /* Write error. */
    /* Set 'error' only if not already set by rdbSaveRio() call. */
    if (error && *error == 0) *error = errno;
    return C_ERR;
}

/* Save the DB on disk. Return C_ERR on error, C_OK on success. */
//将数据库保存在磁盘上，返回C_OK成功，否则返回C_ERR
int rdbSave(char *filename, rdbSaveInfo *rsi) {
    char tmpfile[256];
    char cwd[MAXPATHLEN]; /* Current working dir path for error messages. */
    FILE *fp;
    rio rdb;
    int error = 0;

    //拼接获取对应的备份临时文件的名称
    snprintf(tmpfile,256,"temp-%d.rdb", (int) getpid());
	//以写方式打开临时文件
    fp = fopen(tmpfile,"w");
	//打开失败，获取文件目录，写入日志
    if (!fp) {
        char *cwdp = getcwd(cwd,MAXPATHLEN);
		//写日志信息到logfile
        serverLog(LL_WARNING,
            "Failed opening the RDB file %s (in server root dir %s) "
            "for saving: %s", filename, cwdp ? cwdp : "unknown", strerror(errno));
	    //返回备份失败的标识
        return C_ERR;
    }

	//初始化一个rio对象，该对象是一个文件对象IO
    rioInitWithFile(&rdb,fp);
	//将库中的内容写到rio中
    if (rdbSaveRio(&rdb,&error,RDB_SAVE_NONE,rsi) == C_ERR) {
        errno = error;
        goto werr;
    }

    /* Make sure data will not remain on the OS's output buffers */
	//冲洗缓冲区，确保所有的数据都写入磁盘
    if (fflush(fp) == EOF) 
		goto werr;
	//将fp指向的文件同步到磁盘中
    if (fsync(fileno(fp)) == -1) 
		goto werr;
	//关闭文件
    if (fclose(fp) == EOF) 
		goto werr;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    //原子性改变rdb文件的名字
    if (rename(tmpfile,filename) == -1) {
        char *cwdp = getcwd(cwd,MAXPATHLEN);
        serverLog(LL_WARNING,
            "Error moving temp DB file %s on the final "
            "destination %s (in server root dir %s): %s",
            tmpfile, filename, cwdp ? cwdp : "unknown", strerror(errno));
	    //删除对应的临时文件
        unlink(tmpfile);
		//返回rdb备份失败的标识
        return C_ERR;
    }

	//写日志文件
    serverLog(LL_NOTICE,"DB saved on disk");
	//重置服务器的脏键
    server.dirty = 0;
	//更新上一次SAVE操作的时间
    server.lastsave = time(NULL);
	//更新SAVE操作的状态
    server.lastbgsave_status = C_OK;
	//返回rdb备份操作成功的标识
    return C_OK;

//rdbSaveRio()函数的写错误处理，写日志，关闭文件，删除临时文件，发送C_ERR
werr:
    serverLog(LL_WARNING,"Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return C_ERR;
}

/* 后台进行RDB持久化BGSAVE操作 */
int rdbSaveBackground(char *filename, rdbSaveInfo *rsi) {
    pid_t childpid;
    long long start;
	
	//当前没有正在进行AOF和RDB操作，否则返回C_ERR
    if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) 
		return C_ERR;

    //备份当前数据库的脏键值
    server.dirty_before_bgsave = server.dirty;
	//最近一个执行BGSAVE的时间
    server.lastbgsave_try = time(NULL);
	//
    openChildInfoPipe();

    //fork函数开始时间，记录fork函数的耗时
    start = ustime();
	//创建子进程
    if ((childpid = fork()) == 0) {
        int retval;

        /* Child 子进程执行的代码 */

	    //关闭监听的套接字
        closeListeningSockets(0);
	    //设置进程标题，方便识别
        redisSetProcTitle("redis-rdb-bgsave");
		//执行保存操作，将数据库的写到filename文件中
        retval = rdbSave(filename,rsi);
		//检测是否保存成功
        if (retval == C_OK) {
			//得到子进程进程的脏私有虚拟页面大小，如果做RDB的同时父进程正在写入的数据，那么子进程就会拷贝一个份父进程的内存，而不是和父进程共享一份内存
            size_t private_dirty = zmalloc_get_private_dirty(-1);
			//将子进程分配的内容写日志
            if (private_dirty) {
                serverLog(LL_NOTICE,"RDB: %zu MB of memory used by copy-on-write",private_dirty/(1024*1024));
            }
			//
            server.child_info_data.cow_size = private_dirty;
			//
            sendChildInfo(CHILD_INFO_TYPE_RDB);
        }
		//子进程退出，发送信号给父进程，发送0表示BGSAVE成功，1表示失败
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent 父进程执行的代码 */
        server.stat_fork_time = ustime()-start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);
        if (childpid == -1) {
            closeChildInfoPipe();
            server.lastbgsave_status = C_ERR;
            serverLog(LL_WARNING,"Can't save in background: fork: %s",strerror(errno));
            return C_ERR;
        }
        serverLog(LL_NOTICE,"Background saving started by pid %d",childpid);
        server.rdb_save_time_start = time(NULL);
        server.rdb_child_pid = childpid;
        server.rdb_child_type = RDB_CHILD_TYPE_DISK;
        updateDictResizePolicy();
        return C_OK;
    }
    return C_OK; /* unreached */
}

void rdbRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile,sizeof(tmpfile),"temp-%d.rdb", (int) childpid);
    unlink(tmpfile);
}

/* This function is called by rdbLoadObject() when the code is in RDB-check
 * mode and we find a module value of type 2 that can be parsed without
 * the need of the actual module. The value is parsed for errors, finally
 * a dummy redis object is returned just to conform to the API. */
robj *rdbLoadCheckModuleValue(rio *rdb, char *modulename) {
    uint64_t opcode;
    while((opcode = rdbLoadLen(rdb,NULL)) != RDB_MODULE_OPCODE_EOF) {
        if (opcode == RDB_MODULE_OPCODE_SINT || opcode == RDB_MODULE_OPCODE_UINT){
            uint64_t len;
            if (rdbLoadLenByRef(rdb,NULL,&len) == -1) {
                rdbExitReportCorruptRDB("Error reading integer from module %s value", modulename);
            }
        } else if (opcode == RDB_MODULE_OPCODE_STRING) {
            robj *o = rdbGenericLoadStringObject(rdb,RDB_LOAD_NONE,NULL);
            if (o == NULL) {
                rdbExitReportCorruptRDB("Error reading string from module %s value", modulename);
            }
            decrRefCount(o);
        } else if (opcode == RDB_MODULE_OPCODE_FLOAT) {
            float val;
            if (rdbLoadBinaryFloatValue(rdb,&val) == -1) {
                rdbExitReportCorruptRDB("Error reading float from module %s value", modulename);
            }
        } else if (opcode == RDB_MODULE_OPCODE_DOUBLE) {
            double val;
            if (rdbLoadBinaryDoubleValue(rdb,&val) == -1) {
                rdbExitReportCorruptRDB("Error reading double from module %s value", modulename);
            }
        }
    }
    return createStringObject("module-dummy-value",18);
}

/* Load a Redis object of the specified type from the specified file.
 * On success a newly allocated object is returned, otherwise NULL. */
robj *rdbLoadObject(int rdbtype, rio *rdb) {
    robj *o = NULL, *ele, *dec;
    uint64_t len;
    unsigned int i;

    if (rdbtype == RDB_TYPE_STRING) {
        /* Read string value */
        if ((o = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        o = tryObjectEncoding(o);
    } else if (rdbtype == RDB_TYPE_LIST) {
        /* Read list value */
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;

        o = createQuicklistObject();
        quicklistSetOptions(o->ptr, server.list_max_ziplist_size,server.list_compress_depth);

        /* Load every single element of the list */
        while(len--) {
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            dec = getDecodedObject(ele);
            size_t len = sdslen(dec->ptr);
            quicklistPushTail(o->ptr, dec->ptr, len);
            decrRefCount(dec);
            decrRefCount(ele);
        }
    } else if (rdbtype == RDB_TYPE_SET) {
        /* Read Set value */
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) return NULL;

        /* Use a regular set when there are too many entries. */
        if (len > server.set_max_intset_entries) {
            o = createSetObject();
            /* It's faster to expand the dict to the right size asap in order
             * to avoid rehashing */
            if (len > DICT_HT_INITIAL_SIZE)
                dictExpand(o->ptr,len);
        } else {
            o = createIntsetObject();
        }

        /* Load every single element of the set */
        for (i = 0; i < len; i++) {
            long long llval;
            sds sdsele;

            if ((sdsele = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) 
				return NULL;

            if (o->encoding == OBJ_ENCODING_INTSET) {
                /* Fetch integer value from element. */
                if (isSdsRepresentableAsLongLong(sdsele,&llval) == C_OK) {
                    o->ptr = intsetAdd(o->ptr,llval,NULL);
                } else {
                    setTypeConvert(o,OBJ_ENCODING_HT);
                    dictExpand(o->ptr,len);
                }
            }

            /* This will also be called when the set was just converted
             * to a regular hash table encoded set. */
            if (o->encoding == OBJ_ENCODING_HT) {
                dictAdd((dict*)o->ptr,sdsele,NULL);
            } else {
                sdsfree(sdsele);
            }
        }
    } else if (rdbtype == RDB_TYPE_ZSET_2 || rdbtype == RDB_TYPE_ZSET) {
        /* Read list/set value. */
        uint64_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;

        if ((zsetlen = rdbLoadLen(rdb,NULL)) == RDB_LENERR) 
			return NULL;
        o = createZsetObject();
        zs = o->ptr;

        /* Load every single element of the sorted set. */
        while(zsetlen--) {
            sds sdsele;
            double score;
            zskiplistNode *znode;

            if ((sdsele = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) 
				return NULL;

            if (rdbtype == RDB_TYPE_ZSET_2) {
                if (rdbLoadBinaryDoubleValue(rdb,&score) == -1) 
					return NULL;
            } else {
                if (rdbLoadDoubleValue(rdb,&score) == -1) 
					return NULL;
            }

            /* Don't care about integer-encoded strings. */
            if (sdslen(sdsele) > maxelelen) 
				maxelelen = sdslen(sdsele);

            znode = zslInsert(zs->zsl,score,sdsele);
            dictAdd(zs->dict,sdsele,&znode->score);
        }

        /* Convert *after* loading, since sorted sets are not stored ordered. */
        if (zsetLength(o) <= server.zset_max_ziplist_entries && maxelelen <= server.zset_max_ziplist_value)
            zsetConvert(o,OBJ_ENCODING_ZIPLIST);
    } else if (rdbtype == RDB_TYPE_HASH) {
        uint64_t len;
        int ret;
        sds field, value;

        len = rdbLoadLen(rdb, NULL);
        if (len == RDB_LENERR) return NULL;

        o = createHashObject();

        /* Too many entries? Use a hash table. */
        if (len > server.hash_max_ziplist_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);

        /* Load every field and value into the ziplist */
        while (o->encoding == OBJ_ENCODING_ZIPLIST && len > 0) {
            len--;
            /* Load raw strings */
            if ((field = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) 
				return NULL;
            if ((value = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) 
				return NULL;

            /* Add pair to ziplist */
            o->ptr = ziplistPush(o->ptr, (unsigned char*)field, sdslen(field), ZIPLIST_TAIL);
            o->ptr = ziplistPush(o->ptr, (unsigned char*)value, sdslen(value), ZIPLIST_TAIL);

            /* Convert to hash table if size threshold is exceeded */
            if (sdslen(field) > server.hash_max_ziplist_value || sdslen(value) > server.hash_max_ziplist_value) {
                sdsfree(field);
                sdsfree(value);
                hashTypeConvert(o, OBJ_ENCODING_HT);
                break;
            }
            sdsfree(field);
            sdsfree(value);
        }

        /* Load remaining fields and values into the hash table */
        while (o->encoding == OBJ_ENCODING_HT && len > 0) {
            len--;
            /* Load encoded strings */
            if ((field = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) 
				return NULL;
            if ((value = rdbGenericLoadStringObject(rdb,RDB_LOAD_SDS,NULL)) == NULL) 
				return NULL;

            /* Add pair to hash table */
            ret = dictAdd((dict*)o->ptr, field, value);
            if (ret == DICT_ERR) {
                rdbExitReportCorruptRDB("Duplicate keys detected");
            }
        }

        /* All pairs should be read by now */
        serverAssert(len == 0);
    } else if (rdbtype == RDB_TYPE_LIST_QUICKLIST) {
        if ((len = rdbLoadLen(rdb,NULL)) == RDB_LENERR) 
			return NULL;
        o = createQuicklistObject();
        quicklistSetOptions(o->ptr, server.list_max_ziplist_size, server.list_compress_depth);

        while (len--) {
            unsigned char *zl = rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,NULL);
            if (zl == NULL) 
				return NULL;
            quicklistAppendZiplist(o->ptr, zl);
        }
    } else if (rdbtype == RDB_TYPE_HASH_ZIPMAP  ||
               rdbtype == RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == RDB_TYPE_SET_INTSET   ||
               rdbtype == RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == RDB_TYPE_HASH_ZIPLIST)
    {
        unsigned char *encoded = rdbGenericLoadStringObject(rdb,RDB_LOAD_PLAIN,NULL);
        if (encoded == NULL) 
			return NULL;
        o = createObject(OBJ_STRING,encoded); /* Obj type fixed below. */

        /* Fix the object encoding, and make sure to convert the encoded
         * data type into the base type if accordingly to the current
         * configuration there are too many elements in the encoded data
         * type. Note that we only check the length and not max element
         * size as this is an O(N) scan. Eventually everything will get
         * converted. */
        switch(rdbtype) {
            case RDB_TYPE_HASH_ZIPMAP:
                /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
                {
                    unsigned char *zl = ziplistNew();
                    unsigned char *zi = zipmapRewind(o->ptr);
                    unsigned char *fstr, *vstr;
                    unsigned int flen, vlen;
                    unsigned int maxlen = 0;

                    while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                        if (flen > maxlen) maxlen = flen;
                        if (vlen > maxlen) maxlen = vlen;
                        zl = ziplistPush(zl, fstr, flen, ZIPLIST_TAIL);
                        zl = ziplistPush(zl, vstr, vlen, ZIPLIST_TAIL);
                    }

                    zfree(o->ptr);
                    o->ptr = zl;
                    o->type = OBJ_HASH;
                    o->encoding = OBJ_ENCODING_ZIPLIST;

                    if (hashTypeLength(o) > server.hash_max_ziplist_entries ||
                        maxlen > server.hash_max_ziplist_value)
                    {
                        hashTypeConvert(o, OBJ_ENCODING_HT);
                    }
                }
                break;
            case RDB_TYPE_LIST_ZIPLIST:
                o->type = OBJ_LIST;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                listTypeConvert(o,OBJ_ENCODING_QUICKLIST);
                break;
            case RDB_TYPE_SET_INTSET:
                o->type = OBJ_SET;
                o->encoding = OBJ_ENCODING_INTSET;
                if (intsetLen(o->ptr) > server.set_max_intset_entries)
                    setTypeConvert(o,OBJ_ENCODING_HT);
                break;
            case RDB_TYPE_ZSET_ZIPLIST:
                o->type = OBJ_ZSET;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                if (zsetLength(o) > server.zset_max_ziplist_entries)
                    zsetConvert(o,OBJ_ENCODING_SKIPLIST);
                break;
            case RDB_TYPE_HASH_ZIPLIST:
                o->type = OBJ_HASH;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                if (hashTypeLength(o) > server.hash_max_ziplist_entries)
                    hashTypeConvert(o, OBJ_ENCODING_HT);
                break;
            default:
                rdbExitReportCorruptRDB("Unknown RDB encoding type %d",rdbtype);
                break;
        }
    } else if (rdbtype == RDB_TYPE_MODULE || rdbtype == RDB_TYPE_MODULE_2) {
        uint64_t moduleid = rdbLoadLen(rdb,NULL);
        moduleType *mt = moduleTypeLookupModuleByID(moduleid);
        char name[10];

        if (rdbCheckMode && rdbtype == RDB_TYPE_MODULE_2)
            return rdbLoadCheckModuleValue(rdb,name);

        if (mt == NULL) {
            moduleTypeNameByID(name,moduleid);
            serverLog(LL_WARNING,"The RDB file contains module data I can't load: no matching module '%s'", name);
            exit(1);
        }
        RedisModuleIO io;
        moduleInitIOContext(io,mt,rdb);
        io.ver = (rdbtype == RDB_TYPE_MODULE) ? 1 : 2;
        /* Call the rdb_load method of the module providing the 10 bit
         * encoding version in the lower 10 bits of the module ID. */
        void *ptr = mt->rdb_load(&io,moduleid&1023);
        if (io.ctx) {
            moduleFreeContext(io.ctx);
            zfree(io.ctx);
        }

        /* Module v2 serialization has an EOF mark at the end. */
        if (io.ver == 2) {
            uint64_t eof = rdbLoadLen(rdb,NULL);
            if (eof != RDB_MODULE_OPCODE_EOF) {
                serverLog(LL_WARNING,"The RDB file contains module data for the module '%s' that is not terminated by the proper module value EOF marker", name);
                exit(1);
            }
        }

        if (ptr == NULL) {
            moduleTypeNameByID(name,moduleid);
            serverLog(LL_WARNING,"The RDB file contains module data for the module type '%s', that the responsible module is not able to load. Check for modules log above for additional clues.", name);
            exit(1);
        }
        o = createModuleObject(mt,ptr);
    } else {
        rdbExitReportCorruptRDB("Unknown RDB encoding type %d",rdbtype);
    }
    return o;
}

/* 设置载入时服务器的状态信息
 * Mark that we are loading in the global state and setup the fields needed to provide loading stats. 
 */
void startLoading(FILE *fp) {
    struct stat sb;

    /* Load the DB */
	//正在载入状态
    server.loading = 1;
	//载入开始时间
    server.loading_start_time = time(NULL);
	//已载入的字节数
    server.loading_loaded_bytes = 0;
	//读出文件的信息
    if (fstat(fileno(fp), &sb) == -1) {
        server.loading_total_bytes = 0;
    } else {
        //设置载入的总字节
        server.loading_total_bytes = sb.st_size;
    }
}

/* Refresh the loading progress info */
void loadingProgress(off_t pos) {
    server.loading_loaded_bytes = pos;
    if (server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

/* 设置载入完成的状态
 * Loading finished 
 */
void stopLoading(void) {
    //设置载入数据完成标记位
    server.loading = 0;
}

/* Track loading progress in order to serve client's from time to time
   and if needed calculate rdb checksum  */
void rdbLoadProgressCallback(rio *r, const void *buf, size_t len) {
    if (server.rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    if (server.loading_process_events_interval_bytes &&
        (r->processed_bytes + len)/server.loading_process_events_interval_bytes > r->processed_bytes/server.loading_process_events_interval_bytes)
    {
        /* The DB can take some non trivial amount of time to load. Update
         * our cached time since it is used to create and update the last
         * interaction time with clients and for other important things. */
        updateCachedTime();
        if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER)
            replicationSendNewlineToMaster();
        loadingProgress(r->processed_bytes);
        processEventsWhileBlocked();
    }
}

/* 通过rdb文件进行数据载入处理
 * Load an RDB file from the rio stream 'rdb'. On success C_OK is returned,
 * otherwise C_ERR is returned and 'errno' is set accordingly. 
 */
int rdbLoadRio(rio *rdb, rdbSaveInfo *rsi, int loading_aof) {
    uint64_t dbid;
    int type, rdbver;
    redisDb *db = server.db+0;
    char buf[1024];
    long long expiretime, now = mstime();

    //设置计算校验和的函数
    rdb->update_cksum = rdbLoadProgressCallback;
	//设置载入读或写的最大字节数，2M
    rdb->max_processing_chunk = server.loading_process_events_interval_bytes;
	//读出9个字节到buf，buf中保存着Redis版本"redis0007"
    if (rioRead(rdb,buf,9) == 0) 
		goto eoferr;
	//设置结束标记位 "redis0007\0"
    buf[9] = '\0';
	//检查读出的版本号标识
    if (memcmp(buf,"REDIS",5) != 0) {
        serverLog(LL_WARNING,"Wrong signature trying to load DB from file");
		//读出的值非法错误标识
        errno = EINVAL;
        return C_ERR;
    }
	//转换成整数检查版本大小
    rdbver = atoi(buf+5);
	//检测对应的版本值是否合法
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        serverLog(LL_WARNING,"Can't handle RDB format version %d",rdbver);
        errno = EINVAL;
        return C_ERR;
    }

	//开始读取RDB文件到数据库中
    while(1) {
        robj *key, *val;
		//记录对应的过期时间值
        expiretime = -1;

        //首先读出类型
        if ((type = rdbLoadType(rdb)) == -1) 
			goto eoferr;

        /* Handle special types. */
		//处理特殊情况
        if (type == RDB_OPCODE_EXPIRETIME) {
            /* EXPIRETIME: load an expire associated with the next key
             * to load. Note that after loading an expire we need to load the actual type, and continue. 
             */
            //如果首先是读出过期时间单位为秒
            //从rio中读出过期时间
            if ((expiretime = rdbLoadTime(rdb)) == -1) 
				goto eoferr;
            /* We read the time so we need to read the object type again. */
			//从过期时间后读出一个键值对的类型
            if ((type = rdbLoadType(rdb)) == -1) 
				goto eoferr;
            /* the EXPIRETIME opcode specifies time in seconds, so convert
             * into milliseconds. */
            //转换成毫秒
            expiretime *= 1000;
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            /* EXPIRETIME_MS: milliseconds precision expire times introduced
             * with RDB v3. Like EXPIRETIME but no with more precision. 
             */
            //读出过期时间单位为毫秒
            //从rio中读出过期时间
            if ((expiretime = rdbLoadMillisecondTime(rdb)) == -1) 
				goto eoferr;
            /* We read the time so we need to read the object type again. */
			//从过期时间后读出一个键值对的类型
            if ((type = rdbLoadType(rdb)) == -1) 
				goto eoferr;
        } else if (type == RDB_OPCODE_EOF) {
            //如果读到EOF，则直接跳出循环--------------------->此标记标识所有的加载数据操作已经完成了
            /* EOF: End of file, exit the main loop. */
            break;
        } else if (type == RDB_OPCODE_SELECTDB) {
            /* SELECTDB: Select the specified database. */
		    //读出的是切换数据库操作
		    //读取出一个长度，保存的是数据库的ID
            if ((dbid = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
			//检查读出的ID是否合法
            if (dbid >= (unsigned)server.dbnum) {
                serverLog(LL_WARNING,
                    "FATAL: Data file was created with a Redis "
                    "server configured to handle more than %d "
                    "databases. Exiting\n", server.dbnum);
                exit(1);
            }
			//获取对应索引的库对象
            db = server.db+dbid;
			//跳过本层循环，在读一个type
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_RESIZEDB) {
            /* RESIZEDB: Hint about the size of the keys in the currently selected data base, in order to avoid useless rehashing. */
		    //如果读出调整哈希表的操作
			uint64_t db_size, expires_size;
			//读出一个数据库键值对字典的大小
            if ((db_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
			//读出一个数据库过期字典的大小
            if ((expires_size = rdbLoadLen(rdb,NULL)) == RDB_LENERR)
                goto eoferr;
			//扩展两个字典----------------->此处的好处是,后续不需要进行添加元素时进行空间扩展操作处理了
            dictExpand(db->dict,db_size);
            dictExpand(db->expires,expires_size);
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_AUX) {
            /* AUX: generic string-string fields. Use to add state to RDB
             * which is backward compatible. Implementations of RDB loading
             * are requierd to skip AUX fields they don't understand.
             *
             * An AUX field is composed of two strings: key and value. */
            //读出的是一个辅助字段
            robj *auxkey, *auxval;
			//读出辅助字段的键对象和值对象
            if ((auxkey = rdbLoadStringObject(rdb)) == NULL) 
				goto eoferr;
            if ((auxval = rdbLoadStringObject(rdb)) == NULL) 
				goto eoferr;
			
            //根据对应的键对象的值来区分对应的操作 
            if (((char*)auxkey->ptr)[0] == '%') {
                /* All the fields with a name staring with '%' are considered
                 * information fields and are logged at startup with a log level of NOTICE. */
                //键对象的第一个字符是%
                //写日志信息
                serverLog(LL_NOTICE,"RDB '%s': %s",(char*)auxkey->ptr,(char*)auxval->ptr);
            } else if (!strcasecmp(auxkey->ptr,"repl-stream-db")) {
                if (rsi) 
					rsi->repl_stream_db = atoi(auxval->ptr);
            } else if (!strcasecmp(auxkey->ptr,"repl-id")) {
                if (rsi && sdslen(auxval->ptr) == CONFIG_RUN_ID_SIZE) {
                    memcpy(rsi->repl_id,auxval->ptr,CONFIG_RUN_ID_SIZE+1);
                    rsi->repl_id_is_set = 1;
                }
            } else if (!strcasecmp(auxkey->ptr,"repl-offset")) {
                if (rsi) 
					rsi->repl_offset = strtoll(auxval->ptr,NULL,10);
            } else if (!strcasecmp(auxkey->ptr,"lua")) {
                /* Load the script back in memory. */
			    //加载对应的lua脚本到内存中
			    //创建对应的lua处理函数
                if (luaCreateFunction(NULL,server.lua,auxval) == NULL) {
                    rdbExitReportCorruptRDB("Can't load Lua script from RDB file! " "BODY: %s", auxval->ptr);
                }
            } else {
                /* We ignore fields we don't understand, as by AUX field contract. */
                serverLog(LL_DEBUG,"Unrecognized RDB AUX field: '%s'",(char*)auxkey->ptr);
            }

			//释放对应的键值对对象
            decrRefCount(auxkey);
            decrRefCount(auxval);
            continue; /* Read type again. */
        }

        /* Read key */
		//读出一个key对象
        if ((key = rdbLoadStringObject(rdb)) == NULL) 
			goto eoferr;
        /* Read value */
		//读出一个val对象
        if ((val = rdbLoadObject(type,rdb)) == NULL) 
			goto eoferr;
        /* Check if the key already expired. This function is used when loading
         * an RDB file from disk, either at startup, or when an RDB was
         * received from the master. In the latter case, the master is
         * responsible for key expiry. If we would expire keys here, the
         * snapshot taken by the master may not be reflected on the slave. */
        //如果当前环境不是从节点，且该键设置了过期时间，已经过期
        if (server.masterhost == NULL && !loading_aof && expiretime != -1 && expiretime < now) {
			//释放键值对
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }
        /* Add the new object in the hash table */
		//将没有过期的键值对添加到数据库键值对字典中
        dbAdd(db,key,val);

        /* Set the expire time if needed */
		//如果需要，设置过期时间
        if (expiretime != -1) 
			setExpire(NULL,db,key,expiretime);
		//释放临时对象
        decrRefCount(key);
    }
    /* Verify the checksum if RDB version is >= 5 */
	//此时已经读出完所有数据库的键值对，读到了EOF，但是EOF不是RDB文件的结束，还要进行校验和
	//当RDB版本大于5时，且开启了校验和的功能，那么进行校验和
    if (rdbver >= 5) {
        uint64_t cksum, expected = rdb->cksum;
		//读出一个8字节的校验和，然后比较
        if (rioRead(rdb,&cksum,8) == 0) 
			goto eoferr;
		//检测服务器是否配置了需要进行校验和检验操作处理
        if (server.rdb_checksum) {
            memrev64ifbe(&cksum);
            if (cksum == 0) {
                serverLog(LL_WARNING,"RDB file was saved with checksum disabled: no check performed.");
            } else if (cksum != expected) {
                serverLog(LL_WARNING,"Wrong RDB checksum. Aborting now.");
                rdbExitReportCorruptRDB("RDB CRC error");
            }
        }
    }
	//返回加载数据成功的标识
    return C_OK;

eoferr: /* unexpected end of file is handled here with a fatal exit */
    serverLog(LL_WARNING,"Short read or OOM loading DB. Unrecoverable error, aborting now.");
	//检查rdb错误发送信息且退出
    rdbExitReportCorruptRDB("Unexpected EOF reading RDB file");
	//返回加载数据失败的错误标识
    return C_ERR; /* Just to avoid warning */
}

/* 将指定的RDB文件读到数据库中
 * Like rdbLoadRio() but takes a filename instead of a rio stream. The
 * filename is open for reading and a rio stream object created in order
 * to do the actual loading. Moreover the ETA displayed in the INFO
 * output is initialized and finalized.
 *
 * If you pass an 'rsi' structure initialied with RDB_SAVE_OPTION_INIT, the
 * loading code will fiil the information fields in the structure. 
 */
int rdbLoad(char *filename, rdbSaveInfo *rsi) {
    FILE *fp;
    rio rdb;
    int retval;
	//以只读方式打开文件
    if ((fp = fopen(filename,"r")) == NULL) 
		return C_ERR;
	//设置载入数据时server的状态信息
    startLoading(fp);
	//初始化一个文件流对象rio且设置对应文件指针
    rioInitWithFile(&rdb,fp);
	//通过rdb文件进行数据载入处理
    retval = rdbLoadRio(&rdb,rsi,0);
	//载入完成关闭对应的文件
    fclose(fp);
	//设置载入完成的状态
    stopLoading();
	//返回载入数据是否成功的标识
    return retval;
}

/*
 * A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of actual BGSAVEs.
 */
// 处理子进程进行BGSAVE完成时，要发送的实际信号
// BGSAVE的类型是写入磁盘的
// exitcode是子进程退出时的退出码，成功退出为0
// bysignal 子进程接受到信号
void backgroundSaveDoneHandlerDisk(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        server.lastbgsave_status = C_OK;
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background saving error");
        server.lastbgsave_status = C_ERR;
    } else {
        mstime_t latency;

        serverLog(LL_WARNING,
            "Background saving terminated by signal %d", bysignal);
        latencyStartMonitor(latency);
        rdbRemoveTempFile(server.rdb_child_pid);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("rdb-unlink-temp-file",latency);
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * tirggering an error conditon. */
        if (bysignal != SIGUSR1)
            server.lastbgsave_status = C_ERR;
    }
    server.rdb_child_pid = -1;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_last = time(NULL)-server.rdb_save_time_start;
    server.rdb_save_time_start = -1;
    /* Possibly there are slaves waiting for a BGSAVE in order to be served
     * (the first stage of SYNC is a bulk transfer of dump.rdb) */
    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, RDB_CHILD_TYPE_DISK);
}

/* 
 * A background saving child (BGSAVE) terminated its work. Handle this.
 * This function covers the case of RDB -> Salves socket transfers for diskless replication. 
 */
// 处理子进程进行BGSAVE完成时，要发送的实际信号
// BGSAVE的类型是写入从节点的socket的
// exitcode是子进程退出时的退出码，成功退出为0
// bysignal 子进程接受到信号
void backgroundSaveDoneHandlerSocket(int exitcode, int bysignal) {
    uint64_t *ok_slaves;

    if (!bysignal && exitcode == 0) {
        serverLog(LL_NOTICE,
            "Background RDB transfer terminated with success");
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background transfer error");
    } else {
        serverLog(LL_WARNING,
            "Background transfer terminated by signal %d", bysignal);
    }
    server.rdb_child_pid = -1;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_start = -1;

    /* If the child returns an OK exit code, read the set of slave client
     * IDs and the associated status code. We'll terminate all the slaves
     * in error state.
     *
     * If the process returned an error, consider the list of slaves that
     * can continue to be emtpy, so that it's just a special case of the
     * normal code path. */
    ok_slaves = zmalloc(sizeof(uint64_t)); /* Make space for the count. */
    ok_slaves[0] = 0;
    if (!bysignal && exitcode == 0) {
        int readlen = sizeof(uint64_t);

        if (read(server.rdb_pipe_read_result_from_child, ok_slaves, readlen) == readlen) {
            readlen = ok_slaves[0]*sizeof(uint64_t)*2;

            /* Make space for enough elements as specified by the first
             * uint64_t element in the array. */
            ok_slaves = zrealloc(ok_slaves,sizeof(uint64_t)+readlen);
            if (readlen && read(server.rdb_pipe_read_result_from_child, ok_slaves+1, readlen) != readlen){
                ok_slaves[0] = 0;
            }
        }
    }

    close(server.rdb_pipe_read_result_from_child);
    close(server.rdb_pipe_write_result_to_parent);

    /* We can continue the replication process with all the slaves that
     * correctly received the full payload. Others are terminated. */
    listNode *ln;
    listIter li;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            uint64_t j;
            int errorcode = 0;

            /* Search for the slave ID in the reply. In order for a slave to
             * continue the replication process, we need to find it in the list,
             * and it must have an error code set to 0 (which means success). */
            for (j = 0; j < ok_slaves[0]; j++) {
                if (slave->id == ok_slaves[2*j+1]) {
                    errorcode = ok_slaves[2*j+2];
                    break; /* Found in slaves list. */
                }
            }
            if (j == ok_slaves[0] || errorcode != 0) {
                serverLog(LL_WARNING,
                "Closing slave %s: child->slave RDB transfer failed: %s",
                    replicationGetSlaveName(slave),
                    (errorcode == 0) ? "RDB transfer child aborted"
                                     : strerror(errorcode));
                freeClient(slave);
            } else {
                serverLog(LL_WARNING,
                "Slave %s correctly received the streamed RDB file.",
                    replicationGetSlaveName(slave));
                /* Restore the socket as non-blocking. */
                anetNonBlock(NULL,slave->fd);
                anetSendTimeout(NULL,slave->fd,0);
            }
        }
    }
    zfree(ok_slaves);

    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, RDB_CHILD_TYPE_SOCKET);
}

/* 当BGSAVE 完成RDB文件，要么发送给从节点，要么保存到磁盘，调用正确的处理
 * When a background RDB saving/transfer terminates, call the right handler. 
 */
void backgroundSaveDoneHandler(int exitcode, int bysignal) {
    switch(server.rdb_child_type) {
    case RDB_CHILD_TYPE_DISK:
        backgroundSaveDoneHandlerDisk(exitcode,bysignal);
        break;
    case RDB_CHILD_TYPE_SOCKET:
        backgroundSaveDoneHandlerSocket(exitcode,bysignal);
        break;
    default:
        serverPanic("Unknown RDB child type.");
        break;
    }
}

/* fork一个子进程将rdb写到状态为等待BGSAVE开始的从节点的socket中
 * Spawn an RDB child that writes the RDB to the sockets of the slaves
 * that are currently in SLAVE_STATE_WAIT_BGSAVE_START state. 
 */
int rdbSaveToSlavesSockets(rdbSaveInfo *rsi) {
    int *fds;
    uint64_t *clientids;
    int numfds;
    listNode *ln;
    listIter li;
    pid_t childpid;
    long long start;
    int pipefds[2];

    //首先检测当前是否处于备份中
    if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) 
		return C_ERR;

    /* 
     * Before to fork, create a pipe that will be used in order to
     * send back to the parent the IDs of the slaves that successfully received all the writes. 
     */
    if (pipe(pipefds) == -1) 
		return C_ERR;
    server.rdb_pipe_read_result_from_child = pipefds[0];
    server.rdb_pipe_write_result_to_parent = pipefds[1];

    /* Collect the file descriptors of the slaves we want to transfer
     * the RDB to, which are i WAIT_BGSAVE_START state. */
    fds = zmalloc(sizeof(int)*listLength(server.slaves));
    /* We also allocate an array of corresponding client IDs. This will
     * be useful for the child process in order to build the report
     * (sent via unix pipe) that will be sent to the parent. */
    clientids = zmalloc(sizeof(uint64_t)*listLength(server.slaves));
    numfds = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
            clientids[numfds] = slave->id;
            fds[numfds++] = slave->fd;
            replicationSetupSlaveForFullResync(slave,getPsyncInitialOffset());
            /* Put the socket in blocking mode to simplify RDB transfer.
             * We'll restore it when the children returns (since duped socket
             * will share the O_NONBLOCK attribute with the parent). */
            anetBlock(NULL,slave->fd);
            anetSendTimeout(NULL,slave->fd,server.repl_timeout*1000);
        }
    }

    /* Create the child process. */
    openChildInfoPipe();
    start = ustime();
    if ((childpid = fork()) == 0) {
        /* Child */
        int retval;
        rio slave_sockets;

        rioInitWithFdset(&slave_sockets,fds,numfds);
        zfree(fds);

        closeListeningSockets(0);
        redisSetProcTitle("redis-rdb-to-slaves");

        retval = rdbSaveRioWithEOFMark(&slave_sockets,NULL,rsi);
        if (retval == C_OK && rioFlush(&slave_sockets) == 0)
            retval = C_ERR;

        if (retval == C_OK) {
            size_t private_dirty = zmalloc_get_private_dirty(-1);

            if (private_dirty) {
                serverLog(LL_NOTICE,"RDB: %zu MB of memory used by copy-on-write",private_dirty/(1024*1024));
            }

            server.child_info_data.cow_size = private_dirty;
            sendChildInfo(CHILD_INFO_TYPE_RDB);

            /* If we are returning OK, at least one slave was served
             * with the RDB file as expected, so we need to send a report
             * to the parent via the pipe. The format of the message is:
             *
             * <len> <slave[0].id> <slave[0].error> ...
             *
             * len, slave IDs, and slave errors, are all uint64_t integers,
             * so basically the reply is composed of 64 bits for the len field
             * plus 2 additional 64 bit integers for each entry, for a total
             * of 'len' entries.
             *
             * The 'id' represents the slave's client ID, so that the master
             * can match the report with a specific slave, and 'error' is
             * set to 0 if the replication process terminated with a success
             * or the error code if an error occurred. */
            void *msg = zmalloc(sizeof(uint64_t)*(1+2*numfds));
            uint64_t *len = msg;
            uint64_t *ids = len+1;
            int j, msglen;

            *len = numfds;
            for (j = 0; j < numfds; j++) {
                *ids++ = clientids[j];
                *ids++ = slave_sockets.io.fdset.state[j];
            }

            /* Write the message to the parent. If we have no good slaves or
             * we are unable to transfer the message to the parent, we exit
             * with an error so that the parent will abort the replication
             * process with all the childre that were waiting. */
            msglen = sizeof(uint64_t)*(1+2*numfds);
            if (*len == 0 || write(server.rdb_pipe_write_result_to_parent,msg,msglen)!= msglen) {
                retval = C_ERR;
            }
            zfree(msg);
        }
        zfree(clientids);
        rioFreeFdset(&slave_sockets);
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        /* Parent */
        if (childpid == -1) {
            serverLog(LL_WARNING,"Can't save in background: fork: %s",strerror(errno));

            /* Undo the state change. The caller will perform cleanup on
             * all the slaves in BGSAVE_START state, but an early call to
             * replicationSetupSlaveForFullResync() turned it into BGSAVE_END */
            listRewind(server.slaves,&li);
            while((ln = listNext(&li))) {
                client *slave = ln->value;
                int j;

                for (j = 0; j < numfds; j++) {
                    if (slave->id == clientids[j]) {
                        slave->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
                        break;
                    }
                }
            }
            close(pipefds[0]);
            close(pipefds[1]);
            closeChildInfoPipe();
        } else {
            server.stat_fork_time = ustime()-start;
            server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024*1024*1024); /* GB per second. */
            latencyAddSampleIfNeeded("fork",server.stat_fork_time/1000);

            serverLog(LL_NOTICE,"Background RDB transfer started by pid %d",childpid);
            server.rdb_save_time_start = time(NULL);
            server.rdb_child_pid = childpid;
            server.rdb_child_type = RDB_CHILD_TYPE_SOCKET;
            updateDictResizePolicy();
        }
        zfree(clientids);
        zfree(fds);
        return (childpid == -1) ? C_ERR : C_OK;
    }
    return C_OK; /* Unreached. */
}

/*
 * 执行一个同步保存操作，将当前 Redis 实例的所有数据快照(snapshot)以 RDB 文件的形式保存到硬盘
 * 命令格式
 *     SAVE
 * 返回值
 *     保存成功时返回 OK
 */
void saveCommand(client *c) {
    //检测当前是否处于rdb备份过程中
    if (server.rdb_child_pid != -1) {
		//向客户端响应整处于rdb备份中的提示信息
        addReplyError(c,"Background save already in progress");
		//直接返回不再进行相关处理
        return;
    }
	
    rdbSaveInfo rsi, *rsiptr;
	//
    rsiptr = rdbPopulateSaveInfo(&rsi);
	//执行对应的rdb备份操作处理
    if (rdbSave(server.rdb_filename,rsiptr) == C_OK) {
		//保存成功向客户端响应成功标识
        addReply(c,shared.ok);
    } else {
        //保存失败向客户端响应失败标识
        addReply(c,shared.err);
    }
}

/*
 * 用于在后台异步保存当前数据库的数据到磁盘
 *     BGSAVE 命令执行之后立即返回 OK ，然后 Redis fork 出一个新子进程，原来的 Redis 进程(父进程)继续处理客户端请求，而子进程则负责将数据保存到磁盘，然后退出。
 * 命令格式
 *     BGSAVE [SCHEDULE]
 * 返回值
 *     反馈信息
 */
void bgsaveCommand(client *c) {
    int schedule = 0;

    /* The SCHEDULE option changes the behavior of BGSAVE when an AOF rewrite is in progress. Instead of returning an error a BGSAVE gets scheduled. */
    //检测客户端传入的参数是否正确
	if (c->argc > 1) {
        if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"schedule")) {
			//设置schedule标志
            schedule = 1;
        } else {
            //向客户端响应参数配置错误的响应结果
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);

    if (server.rdb_child_pid != -1) {//检测当前是否处于rdb备份中
		//向客户端响应对应的提示信息
        addReplyError(c,"Background save already in progress");
    } else if (server.aof_child_pid != -1) {//检测当前是否处于aof备份中
        //检测是否设置任务标识位,即等待执行完aof操作之后,进行对应的rdb备份操作处理
        if (schedule) {
			//设置rdb_bgsave_scheduled为1，表示可以执行BGSAVE
            server.rdb_bgsave_scheduled = 1;
			//向客户端响应对应的提示信息
            addReplyStatus(c,"Background saving scheduled");
        } else {
			//向客户端响应正在处于aof备份过程中,不能启动后台rdb备份操作处理的提示
            addReplyError(c,
                "An AOF log rewriting in progress: can't BGSAVE right now. "
                "Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever "
                "possible.");
        }
    } else if (rdbSaveBackground(server.rdb_filename,rsiptr) == C_OK) { //触发执行后台aof备份操作处理
        //向客户端响应开始启动后台备份操作处理
        addReplyStatus(c,"Background saving started");
    } else {
		//向客户端响应进行启动后台备份操作处理错误
        addReply(c,shared.err);
    }
}

/* 
 * Populate the rdbSaveInfo structure used to persist the replication
 * information inside the RDB file. Currently the structure explicitly
 * contains just the currently selected DB from the master stream, however
 * if the rdbSave*() family functions receive a NULL rsi structure also
 * the Replication ID/offset is not saved. The function popultes 'rsi'
 * that is normally stack-allocated in the caller, returns the populated
 * pointer if the instance has a valid master client, otherwise NULL
 * is returned, and the RDB saving will not persist any replication related information. 
 */
rdbSaveInfo *rdbPopulateSaveInfo(rdbSaveInfo *rsi) {
    rdbSaveInfo rsi_init = RDB_SAVE_INFO_INIT;
    *rsi = rsi_init;

    /* If the instance is a master, we can populate the replication info
     * only when repl_backlog is not NULL. If the repl_backlog is NULL,
     * it means that the instance isn't in any replication chains. In this
     * scenario the replication info is useless, because when a slave
     * connects to us, the NULL repl_backlog will trigger a full
     * synchronization, at the same time we will use a new replid and clear
     * replid2. */
    if (!server.masterhost && server.repl_backlog) {
        /* Note that when server.slaveseldb is -1, it means that this master
         * didn't apply any write commands after a full synchronization.
         * So we can let repl_stream_db be 0, this allows a restarted slave
         * to reload replication ID/offset, it's safe because the next write
         * command must generate a SELECT statement. */
        rsi->repl_stream_db = server.slaveseldb == -1 ? 0 : server.slaveseldb;
        return rsi;
    }

    /* If the instance is a slave we need a connected master
     * in order to fetch the currently selected DB. */
    if (server.master) {
        rsi->repl_stream_db = server.master->db->id;
        return rsi;
    }

    /* If we have a cached master we can use it in order to populate the
     * replication selected DB info inside the RDB file: the slave can
     * increment the master_repl_offset only from data arriving from the
     * master, so if we are disconnected the offset in the cached master
     * is valid. */
    if (server.cached_master) {
        rsi->repl_stream_db = server.cached_master->db->id;
        return rsi;
    }
    return NULL;
}





