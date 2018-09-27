/* 
 * The ziplist is a specially encoded dually linked list that is designed
 * to be very memory efficient. It stores both strings and integer values,
 * where integers are encoded as actual integers instead of a series of
 * characters. It allows push and pop operations on either side of the list
 * in O(1) time. However, because every operation requires a reallocation of
 * the memory used by the ziplist, the actual complexity is related to the
 * amount of memory used by the ziplist.
 *
 * ----------------------------------------------------------------------------
 *
 * ZIPLIST OVERALL LAYOUT
 * ======================
 *
 * The general layout of the ziplist is as follows:
 *
 * <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 *
 * NOTE: all fields are stored in little endian, if not specified otherwise.
 *
 * <uint32_t zlbytes> is an unsigned integer to hold the number of bytes that
 * the ziplist occupies, including the four bytes of the zlbytes field itself.
 * This value needs to be stored to be able to resize the entire structure
 * without the need to traverse it first.
 *
 * <uint32_t zltail> is the offset to the last entry in the list. This allows
 * a pop operation on the far side of the list without the need for full
 * traversal.
 *
 * <uint16_t zllen> is the number of entries. When there are more than
 * 2^16-2 entires, this value is set to 2^16-1 and we need to traverse the
 * entire list to know how many items it holds.
 *
 * <uint8_t zlend> is a special entry representing the end of the ziplist.
 * Is encoded as a single byte equal to 255. No other normal entry starts
 * with a byte set to the value of 255.
 *
 * ZIPLIST ENTRIES
 * ===============
 *
 * Every entry in the ziplist is prefixed by metadata that contains two pieces
 * of information. First, the length of the previous entry is stored to be
 * able to traverse the list from back to front. Second, the entry encoding is
 * provided. It represents the entry type, integer or string, and in the case
 * of strings it also represents the length of the string payload.
 * So a complete entry is stored like this:
 *
 * <prevlen> <encoding> <entry-data>
 *
 * Sometimes the encoding represents the entry itself, like for small integers
 * as we'll see later. In such a case the <entry-data> part is missing, and we
 * could have just:
 *
 * <prevlen> <encoding>
 *
 * The length of the previous entry, <prevlen>, is encoded in the following way:
 * If this length is smaller than 254 bytes, it will only consume a single
 * byte representing the length as an unsinged 8 bit integer. When the length
 * is greater than or equal to 254, it will consume 5 bytes. The first byte is
 * set to 254 (FE) to indicate a larger value is following. The remaining 4
 * bytes take the length of the previous entry as value.
 *
 * So practically an entry is encoded in the following way:
 *
 * <prevlen from 0 to 253> <encoding> <entry>
 *
 * Or alternatively if the previous entry length is greater than 253 bytes
 * the following encoding is used:
 *
 * 0xFE <4 bytes unsigned little endian prevlen> <encoding> <entry>
 *
 * The encoding field of the entry depends on the content of the
 * entry. When the entry is a string, the first 2 bits of the encoding first
 * byte will hold the type of encoding used to store the length of the string,
 * followed by the actual length of the string. When the entry is an integer
 * the first 2 bits are both set to 1. The following 2 bits are used to specify
 * what kind of integer will be stored after this header. An overview of the
 * different types and encodings is as follows. The first byte is always enough
 * to determine the kind of entry.
 *
 * |00pppppp| - 1 byte
 *      String value with length less than or equal to 63 bytes (6 bits).
 *      "pppppp" represents the unsigned 6 bit length.
 * |01pppppp|qqqqqqqq| - 2 bytes
 *      String value with length less than or equal to 16383 bytes (14 bits).
 *      IMPORTANT: The 14 bit number is stored in big endian.
 * |10000000|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| - 5 bytes
 *      String value with length greater than or equal to 16384 bytes.
 *      Only the 4 bytes following the first byte represents the length
 *      up to 32^2-1. The 6 lower bits of the first byte are not used and
 *      are set to zero.
 *      IMPORTANT: The 32 bit number is stored in big endian.
 * |11000000| - 3 bytes
 *      Integer encoded as int16_t (2 bytes).
 * |11010000| - 5 bytes
 *      Integer encoded as int32_t (4 bytes).
 * |11100000| - 9 bytes
 *      Integer encoded as int64_t (8 bytes).
 * |11110000| - 4 bytes
 *      Integer encoded as 24 bit signed (3 bytes).
 * |11111110| - 2 bytes
 *      Integer encoded as 8 bit signed (1 byte).
 * |1111xxxx| - (with xxxx between 0000 and 1101) immediate 4 bit integer.
 *      Unsigned integer from 0 to 12. The encoded value is actually from
 *      1 to 13 because 0000 and 1111 can not be used, so 1 should be
 *      subtracted from the encoded 4 bit value to obtain the right value.
 * |11111111| - End of ziplist special entry.
 *
 * Like for the ziplist header, all the integers are represented in little
 * endian byte order, even when this code is compiled in big endian systems.
 *
 * EXAMPLES OF ACTUAL ZIPLISTS
 * ===========================
 *
 * The following is a ziplist containing the two elements representing
 * the strings "2" and "5". It is composed of 15 bytes, that we visually
 * split into sections:
 *
 *  [0f 00 00 00] [0c 00 00 00] [02 00] [00 f3] [02 f6] [ff]
 *        |             |          |       |       |     |
 *     zlbytes        zltail    entries   "2"     "5"   end
 *
 * The first 4 bytes represent the number 15, that is the number of bytes
 * the whole ziplist is composed of. The second 4 bytes are the offset
 * at which the last ziplist entry is found, that is 12, in fact the
 * last entry, that is "5", is at offset 12 inside the ziplist.
 * The next 16 bit integer represents the number of elements inside the
 * ziplist, its value is 2 since there are just two elements inside.
 * Finally "00 f3" is the first entry representing the number 2. It is
 * composed of the previous entry length, which is zero because this is
 * our first entry, and the byte F3 which corresponds to the encoding
 * |1111xxxx| with xxxx between 0001 and 1101. We need to remove the "F"
 * higher order bits 1111, and subtract 1 from the "3", so the entry value
 * is "2". The next entry has a prevlen of 02, since the first entry is
 * composed of exactly two bytes. The entry itself, F6, is encoded exactly
 * like the first entry, and 6-1 = 5, so the value of the entry is 5.
 * Finally the special entry FF signals the end of the ziplist.
 *
 * Adding another element to the above string with the value "Hello World"
 * allows us to show how the ziplist encodes small strings. We'll just show
 * the hex dump of the entry itself. Imagine the bytes as following the
 * entry that stores "5" in the ziplist above:
 *
 * [02] [0b] [48 65 6c 6c 6f 20 57 6f 72 6c 64]
 *
 * The first byte, 02, is the length of the previous entry. The next
 * byte represents the encoding in the pattern |00pppppp| that means
 * that the entry is a string of length <pppppp>, so 0B means that
 * an 11 bytes string follows. From the third byte (48) to the last (64)
 * there are just the ASCII characters for "Hello World".
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <limits.h>
#include "zmalloc.h"
#include "util.h"
#include "ziplist.h"
#include "endianconv.h"
#include "redisassert.h"

/* Special "end of ziplist" entry. */
/*压缩列表最后一个空间位置上的结束标识*/
#define ZIP_END 255  
/* Max number of bytes of the previous entry, for the "prevlen" field prefixing each entry, to be represented with just a single byte. Otherwise it is represented as FF AA BB CC DD, where AA BB CC DD are a 4 bytes unsigned integer representing the previous entry len. */
#define ZIP_BIG_PREVLEN 254 

/* Different encoding/length possibilities */
/*字符串类型对应的掩码 和本掩码与运算处理后,小于本掩码的就是字符串类型*/
#define ZIP_STR_MASK 0xc0                /*  1100 0000  */
#define ZIP_INT_MASK 0x30                /*  0011 0000  */

#define ZIP_STR_06B (0 << 6)             /*  0000 0000  */
#define ZIP_STR_14B (1 << 6)             /*  0001 0000  */
#define ZIP_STR_32B (2 << 6)             /*  0010 0000  */

#define ZIP_INT_16B (0xc0 | 0<<4)        /*  1100 0000  */
#define ZIP_INT_32B (0xc0 | 1<<4)        /*  1101 0000  */
#define ZIP_INT_64B (0xc0 | 2<<4)        /*  1110 0000  */
#define ZIP_INT_24B (0xc0 | 3<<4)        /*  1111 0000  */
#define ZIP_INT_8B 0xfe                  /*  1111 1110  */

/* 
 * 4 bit integer immediate encoding |1111xxxx| with xxxx between 0001 and 1101. 
 */
#define ZIP_INT_IMM_MASK 0x0f   /* Mask to extract the 4 bits value. To add one is needed to reconstruct the value. */
#define ZIP_INT_IMM_MIN 0xf1    /* 11110001 */
#define ZIP_INT_IMM_MAX 0xfd    /* 11111101 */

#define INT24_MAX 0x7fffff
#define INT24_MIN (-INT24_MAX - 1)

/* 根据给定的编码方式检测是否是对应的字符串类型数据
 * Macro to determine if the entry is a string. String entries never start with "11" as most significant bits of the first byte. 
 */
#define ZIP_IS_STR(enc) (((enc) & ZIP_STR_MASK) < ZIP_STR_MASK)

/* Utility macros.*/

/* 获取压缩列表占据的总的字节数量
 * Return total bytes a ziplist is composed of. 
 */
#define ZIPLIST_BYTES(zl)       (*((uint32_t*)(zl)))

/* 获取压缩列表中尾部节点的偏移大小----->偏移的节点个数
 * Return the offset of the last item inside the ziplist. 
 */
#define ZIPLIST_TAIL_OFFSET(zl) (*((uint32_t*)((zl)+sizeof(uint32_t))))

/* 获取压缩列表中元素的个数
 * Return the length of a ziplist, or UINT16_MAX if the length cannot be determined without scanning the whole ziplist.
 */
#define ZIPLIST_LENGTH(zl)      (*((uint16_t*)((zl)+sizeof(uint32_t)*2)))

/* 压缩列表结构中头部占据的字节数量
 * 压缩列表的结构<zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 * The size of a ziplist header: two 32 bit integers for the total
 * bytes count and last item offset. One 16 bit integer for the number of items field. 
 */
#define ZIPLIST_HEADER_SIZE     (sizeof(uint32_t)*2+sizeof(uint16_t))

/* 压缩列表结构中尾部占据的字节数量 其实就是一个字节大小的空间 内部的值始终都是255
 * 压缩列表的结构<zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
 * Size of the "end of ziplist" entry. Just one byte. 
 */
#define ZIPLIST_END_SIZE        (sizeof(uint8_t))

/* 获取压缩列表中第一个元素节点的起始位置指向
 * Return the pointer to the first entry of a ziplist. 
 */
#define ZIPLIST_ENTRY_HEAD(zl)  ((zl)+ZIPLIST_HEADER_SIZE)

/* 获取压缩列表中尾节点的起始位置指向
 * Return the pointer to the last entry of a ziplist, using the last entry offset inside the ziplist header. 
 */
#define ZIPLIST_ENTRY_TAIL(zl)  ((zl)+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)))

/* 获取压缩列表中结束位置的指向
 * Return the pointer to the last byte of a ziplist, which is, the end of ziplist FF entry. 
 */
#define ZIPLIST_ENTRY_END(zl)   ((zl)+intrev32ifbe(ZIPLIST_BYTES(zl))-1)

/* 给压缩列表增加对应数目的元素个数值                   需要注意 此空间中最大存储16bit的元素个数 如果超过了就使用最大值来标识元素个数超过了对应的数目 需要遍历整个压缩列表来获取真正的压缩列表的元素个数
 * Increment the number of items field in the ziplist header. Note that this
 * macro should never overflow the unsigned 16 bit integer, since entires are
 * always pushed one at a time. When UINT16_MAX is reached we want the count
 * to stay there to signal that a full scan is needed to get the number of
 * items inside the ziplist. 
 */
#define ZIPLIST_INCR_LENGTH(zl,incr) {                                                 \
    if (ZIPLIST_LENGTH(zl) < UINT16_MAX)                                               \
        ZIPLIST_LENGTH(zl) = intrev16ifbe(intrev16ifbe(ZIPLIST_LENGTH(zl))+incr);      \
}

/* 用于表示压缩列表中元素节点的实体结构信息结构
 * We use this function to receive information about a ziplist entry.
 * Note that this is not how the data is actually encoded, is just what we
 * get filled by a function in order to operate more easily.
 */
typedef struct zlentry {
    /*前置节点占据空间字节数需要几个字节来表示此大小*/
    unsigned int prevrawlensize; /* Bytes used to encode the previos entry len*/
	/*前置节点占据的空间字节数量*/
    unsigned int prevrawlen;     /* Previous entry len. */
	/*本节点数据部分占据空间字节数需要几个字节来表示此大小*/
    unsigned int lensize;        /* Bytes used to encode this entry type/len. For example strings have a 1, 2 or 5 bytes header. Integers always use a single byte.*/
	/*本节点数据部分占据的空间字节数量*/
    unsigned int len;            /* Bytes used to represent the actual entry. For strings this is just the string length while for integers it is 1, 2, 3, 4, 8 or 0 (for 4 bit immediate) depending on the number range. */
    /*本节点头部一共占据的空间字节数*/
	unsigned int headersize;     /* prevrawlensize + lensize. */
	/*本节点数据的编码方式*/
    unsigned char encoding;      /* Set to ZIP_STR_* or ZIP_INT_* depending on the entry encoding. However for 4 bits immediate integers this can assume a range of values and must be range-checked. */
	/*记录节点的位置指向 注意不是对应的实际数据的位置指向*/
    unsigned char *p;            /* Pointer to the very start of the entry, that is, this points to prev-entry-len field. */
} zlentry;

/*初始化对应的元素节点*/
#define ZIPLIST_ENTRY_ZERO(zle) {                                    \
    (zle)->prevrawlensize = (zle)->prevrawlen = 0;                   \
    (zle)->lensize = (zle)->len = (zle)->headersize = 0;             \
    (zle)->encoding = 0;                                             \
    (zle)->p = NULL;                                                 \
}

/* 获取本节点对应数据的编码方式
 * Extract the encoding from the byte pointed by 'ptr' and set it into 'encoding' field of the zlentry structure. 
 */
#define ZIP_ENTRY_ENCODING(ptr, encoding) do {                    \
    (encoding) = (ptr[0]);                                        \
    if ((encoding) < ZIP_STR_MASK)                                \
		(encoding) &= ZIP_STR_MASK;                               \
} while(0)

/* 根据编码方式获取编码对应的整数数据需要几个字节
 * Return bytes needed to store integer encoded by 'encoding'. 
 */
unsigned int zipIntSize(unsigned char encoding) {
    switch(encoding) {
		case ZIP_INT_8B:  
			return 1;
    	case ZIP_INT_16B: 
			return 2;
   		case ZIP_INT_24B: 
			return 3;
    	case ZIP_INT_32B: 
			return 4;
    	case ZIP_INT_64B: 
			return 8;
    }
	/* 4 bit immediate */
    if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX)
		//对应的数据直接在编码方式中了,所以只需要0个字节
        return 0; 
    panic("Invalid integer encoding 0x%02X", encoding);
    return 0;
}

/* 
 * Write the encoidng header of the entry in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length. Arguments:
 *
 * 'encoding' is the encoding we are using for the entry. It could be
 * ZIP_INT_* or ZIP_STR_* or between ZIP_INT_IMM_MIN and ZIP_INT_IMM_MAX
 * for single-byte small immediate integers.
 *
 * 'rawlen' is only used for ZIP_STR_* encodings and is the length of the
 * srting that this entry represents.
 *
 * The function returns the number of bytes used by the encoding/length
 * header stored in 'p'. 
 */
unsigned int zipStoreEntryEncoding(unsigned char *p, unsigned char encoding, unsigned int rawlen) {
    unsigned char len = 1, buf[5];

    if (ZIP_IS_STR(encoding)) {
        /* Although encoding is given it may not be set for strings, so we determine it here using the raw length. */
        if (rawlen <= 0x3f) {
            if (!p) 
				return len;
            buf[0] = ZIP_STR_06B | rawlen;
        } else if (rawlen <= 0x3fff) {
            len += 1;
            if (!p) 
				return len;
            buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
            buf[1] = rawlen & 0xff;
        } else {
            len += 4;
            if (!p) 
				return len;
            buf[0] = ZIP_STR_32B;
            buf[1] = (rawlen >> 24) & 0xff;
            buf[2] = (rawlen >> 16) & 0xff;
            buf[3] = (rawlen >> 8) & 0xff;
            buf[4] = rawlen & 0xff;
        }
    } else {
        /* Implies integer encoding, so length is always 1. */
        if (!p) 
			return len;
        buf[0] = encoding;
    }

    /* Store this length at p. */
    memcpy(p,buf,len);
    return len;
}

/* 解析本节点元素的编码方式 字节长度 以及使用几个字节来标识对应的字节长度
 * Decode the entry encoding type and data length (string length for strings,
 * number of bytes used for the integer for integer entries) encoded in 'ptr'.
 * The 'encoding' variable will hold the entry encoding, the 'lensize'
 * variable will hold the number of bytes required to encode the entry
 * length, and the 'len' variable will hold the entry length. 
 */
#define ZIP_DECODE_LENGTH(ptr, encoding, lensize, len) do {                    \
    ZIP_ENTRY_ENCODING((ptr), (encoding));                                     \
    if ((encoding) < ZIP_STR_MASK) {                                           \
        if ((encoding) == ZIP_STR_06B) {                                       \
            (lensize) = 1;                                                     \
            (len) = (ptr)[0] & 0x3f;                                           \
        } else if ((encoding) == ZIP_STR_14B) {                                \
            (lensize) = 2;                                                     \
            (len) = (((ptr)[0] & 0x3f) << 8) | (ptr)[1];                       \
        } else if ((encoding) == ZIP_STR_32B) {                                \
            (lensize) = 5;                                                     \
            (len) = ((ptr)[1] << 24) |                                         \
                    ((ptr)[2] << 16) |                                         \
                    ((ptr)[3] <<  8) |                                         \
                    ((ptr)[4]);                                                \
        } else {                                                               \
            panic("Invalid string encoding 0x%02X", (encoding));               \
        }                                                                      \
    } else {                                                                   \
        (lensize) = 1;                                                         \
        (len) = zipIntSize(encoding);                                          \
    }                                                                          \
} while(0);

/* 使用多字节5个字节来表示字符串长度时的处理
 * 注意这个地方可能有一种情况需要额外的注意
 *     这个地方有可能使用了5个字节来表示了一个比较小的长度------>原因可能在插入元素可能导致后续的节点中表示长度的内容变化
 * Encode the length of the previous entry and write it to "p". This only uses the larger encoding (required in __ziplistCascadeUpdate). 
 */
int zipStorePrevEntryLengthLarge(unsigned char *p, unsigned int len) {
    //检测是否设置了记录数据的指针指向
    if (p != NULL) {
		//设置第一个空间中的值的内容
        p[0] = ZIP_BIG_PREVLEN;
		//设置后续的4个字节中对应的表示字符串长度的值
        memcpy(p+1,&len,sizeof(len));
        memrev32ifbe(p+1);
    }
	//返回对应的字节数量为5个字节
    return 1+sizeof(len);
}

/* 获取编码对应长度的字符串所需要的空间字节数量
 * Encode the length of the previous entry and write it to "p". Return the
 * number of bytes needed to encode this length if "p" is NULL. 
 */
unsigned int zipStorePrevEntryLength(unsigned char *p, unsigned int len) {
    //检测是否只是获取对应的表示字节数而不是进行设置数据处理
    if (p == NULL) {
		//在没有设置对应的指针的情况下只是获取对应的需要的字节数量
        return (len < ZIP_BIG_PREVLEN) ? 1 : sizeof(len)+1;
    } else {
        //检测给定的字符串的长度是否小于254
        if (len < ZIP_BIG_PREVLEN) {
			//那么对应的第一个空间就直接设置为字符串所对应的长度
            p[0] = len;
			//同时返回需要的表示字节数量为1
            return 1;
        } else {
			//使用5个字节来记录字符串所对应的长度的处理
            return zipStorePrevEntryLengthLarge(p,len);
        }
    }
}

/* 解析前置节点字节数量能够使用几个字节的空间来标识 短的使用1个字节 0-254 大于254个字节的使用 5个字节来标识
 * Return the number of bytes used to encode the length of the previous entry. The length is returned by setting the var 'prevlensize'. 
 */
#define ZIP_DECODE_PREVLENSIZE(ptr, prevlensize) do {                          \
    if ((ptr)[0] < ZIP_BIG_PREVLEN) {                                          \
        (prevlensize) = 1;                                                     \
    } else {                                                                   \
        (prevlensize) = 5;                                                     \
    }                                                                          \
} while(0);

/* 解析前置节点占据的空间字节数量和对应此空间字节数量需要几个字节来进行表示
 * Return the length of the previous element, and the number of bytes that
 * are used in order to encode the previous element length.
 * 'ptr' must point to the prevlen prefix of an entry (that encodes the
 * length of the previos entry in order to navigate the elements backward).
 * The length of the previous entry is stored in 'prevlen', the number of
 * bytes needed to encode the previous entry length are stored in 'prevlensize'. 
 */
#define ZIP_DECODE_PREVLEN(ptr, prevlensize, prevlen) do {                     \
    ZIP_DECODE_PREVLENSIZE(ptr, prevlensize);                                  \
    if ((prevlensize) == 1) {                                                  \
        (prevlen) = (ptr)[0];                                                  \
    } else if ((prevlensize) == 5) {                                           \
        assert(sizeof((prevlen)) == 4);                                        \
        memcpy(&(prevlen), ((char*)(ptr)) + 1, 4);                             \
        memrev32ifbe(&prevlen);                                                \
    }                                                                          \
} while(0);

/* 计算新的前置节点的字节数量所对应的字节空间数量与原始的前置节点占据字节空间所使用的字节数量的差值
 * Given a pointer 'p' to the prevlen info that prefixes an entry, this
 * function returns the difference in number of bytes needed to encode
 * the prevlen if the previous entry changes of size.
 *
 * So if A is the number of bytes used right now to encode the 'prevlen'
 * field.
 *
 * And B is the number of bytes that are needed in order to encode the
 * 'prevlen' if the previous element will be updated to one of size 'len'.
 *
 * Then the function returns B - A
 *
 * So the function returns a positive number if more space is needed,
 * a negative number if less space is needed, or zero if the same space is needed. 
 */
int zipPrevLenByteDiff(unsigned char *p, unsigned int len) {
    unsigned int prevlensize;
	//获取当前节点的前置节点需要使用几个字节大小来表示其所占据的空间字节数
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);
	//计算新的字节数量对应使用的字节数与先前前置节点使用的字节数表示的差值                              
    return zipStorePrevEntryLength(NULL, len) - prevlensize;
}

/* 获取给定节点占据的总的字节数量
 * Return the total number of bytes used by the entry pointed to by 'p'. 
 */
unsigned int zipRawEntryLength(unsigned char *p) {
    unsigned int prevlensize, encoding, lensize, len;
	//解析表示前置节点字节空间所使用的字节数量
    ZIP_DECODE_PREVLENSIZE(p, prevlensize);
	//解析表示本节点值数据占据空间使用的字节数量和表示此字节数量所使用的字节个数
    ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len);
	//计算本节点一共占据的字节数量
    return prevlensize + lensize + len;
}

/* 尝试将对应的字符串编码成对应的整数类型--------------->目的就是为了减少占据对应的空间数量                                      对应的整数类型比字符串类型占据空间要少好多
 * Check if string pointed to by 'entry' can be encoded as an integer.
 * Stores the integer value in 'v' and its encoding in 'encoding'. 
 */
int zipTryEncoding(unsigned char *entry, unsigned int entrylen, long long *v, unsigned char *encoding) {
    long long value;
	//首先检测给定的字符串的长度是否过长--->超过32位就不需要进行尝试转换处理了
    if (entrylen >= 32 || entrylen == 0) 
		//返回不能进行转换操作处理的标记
		return 0;
	//尝试将对应的字符串类型转换成对应的整数类型
    if (string2ll((char*)entry,entrylen,&value)) {
        /* Great, the string can be encoded. Check what's the smallest of our encoding types that can hold this value. */
        if (value >= 0 && value <= 12) {
			//直接将对应的数据设置到编码方式中
            *encoding = ZIP_INT_IMM_MIN+value;
        } else if (value >= INT8_MIN && value <= INT8_MAX) {
            //设置为1字节的整数编码方式
            *encoding = ZIP_INT_8B;
        } else if (value >= INT16_MIN && value <= INT16_MAX) {
			//设置为2字节的整数编码方式
            *encoding = ZIP_INT_16B;
        } else if (value >= INT24_MIN && value <= INT24_MAX) {
			//设置为3字节的整数编码方式
            *encoding = ZIP_INT_24B;
        } else if (value >= INT32_MIN && value <= INT32_MAX) {
			//设置为4字节的整数编码方式
            *encoding = ZIP_INT_32B;
        } else {
			//设置为8字节的整数编码方式
            *encoding = ZIP_INT_64B;
        }
		//设置对应的整数值,即返回的整数值空间中
        *v = value;
		//设置可以进行转化操作处理的标记
        return 1;
    }
	//返回不能进行转换操作处理的标记
    return 0;
}

/* 根据给定编码方式将对应的整数设置到对应的指针指向的空间中
 * Store integer 'value' at 'p', encoded as 'encoding' 
 */
void zipSaveInteger(unsigned char *p, int64_t value, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64;
    if (encoding == ZIP_INT_8B) {
		//存储8字节整数
        ((int8_t*)p)[0] = (int8_t)value;
    } else if (encoding == ZIP_INT_16B) {
        i16 = value;
		//存储16字节整数
        memcpy(p,&i16,sizeof(i16));
        memrev16ifbe(p);
    } else if (encoding == ZIP_INT_24B) {
        i32 = value<<8;
        memrev32ifbe(&i32);
		//存储24字节整数
        memcpy(p,((uint8_t*)&i32)+1,sizeof(i32)-sizeof(uint8_t));
    } else if (encoding == ZIP_INT_32B) {
        i32 = value;
		//存储32字节整数
        memcpy(p,&i32,sizeof(i32));
        memrev32ifbe(p);
    } else if (encoding == ZIP_INT_64B) {
        i64 = value;
		//存储64字节整数
        memcpy(p,&i64,sizeof(i64));
        memrev64ifbe(p);
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
        /* Nothing to do, the value is stored in the encoding itself. */
	    //此处对应的数据已经记录到了编码方式中了
    } else {
        assert(NULL);
    }
}

/* 根据给定的编码方式来解析给定指针位置开始的整数
 * Read integer encoded as 'encoding' from 'p' 
 */
int64_t zipLoadInteger(unsigned char *p, unsigned char encoding) {
    int16_t i16;
    int32_t i32;
    int64_t i64, ret = 0;
    if (encoding == ZIP_INT_8B) {
		//直接获取对应的8位整数
        ret = ((int8_t*)p)[0];
    } else if (encoding == ZIP_INT_16B) {
        //将对应的数据拷贝到16整数空间中
        memcpy(&i16,p,sizeof(i16));
        memrev16ifbe(&i16);
		//获取对应的16位整数
        ret = i16;
    } else if (encoding == ZIP_INT_32B) {
		//将对应的数据拷贝到32整数空间中
        memcpy(&i32,p,sizeof(i32));
        memrev32ifbe(&i32);
		//获取对应的32位整数
        ret = i32;
    } else if (encoding == ZIP_INT_24B) {
        i32 = 0;
		//此处使用一个32位字节空间来存储24位大小的整数  所以前8个字节暂时没有使用
        memcpy(((uint8_t*)&i32)+1,p,sizeof(i32)-sizeof(uint8_t));
        memrev32ifbe(&i32);
        ret = i32>>8;
    } else if (encoding == ZIP_INT_64B) {
		//将对应的数据拷贝到64整数空间中
        memcpy(&i64,p,sizeof(i64));
        memrev64ifbe(&i64);
		//获取对应的64位整数
        ret = i64;
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
		//此处是直接将数据放置到了编码方式中了--------------->对空间利用的太牛叉了                              一分一厘的空间都要充分利用
        ret = (encoding & ZIP_INT_IMM_MASK)-1;
    } else {
        assert(NULL);
    }
	//返回对应的整数
    return ret;
}

/* 解析给定指针位置上的节点结构信息------>即将对应的一组字节数据转换成对应的实体节点数据类型  ---->方便后期处理
 * Return a struct with all information about an entry. 
 */
void zipEntry(unsigned char *p, zlentry *e) {
    //解析前置节点占据的空间数量和对应此空间字节数需要几个字节来进行表示的数量
    ZIP_DECODE_PREVLEN(p, e->prevrawlensize, e->prevrawlen);
	//解析本节点元素的编码方式 字节长度 以及使用几个字节来标识对应的字节长度
    ZIP_DECODE_LENGTH(p + e->prevrawlensize, e->encoding, e->lensize, e->len);
	//计算节点中头部占据的字节个数                即 前置节点占据空间字节数和本节点占据空间字节数使用多少字节来表示
    e->headersize = e->prevrawlensize + e->lensize;
	//记录对应的节点位置指向
    e->p = p;
}

/* 创建一个压缩机构的列表结构                主要是分配空间同时完成空间结构中的数据初始化处理
 * Create a new empty ziplist. 
 */
unsigned char *ziplistNew(void) {
    //计算并获取需要的空间字节数                <zlbytes> <zltail> <zllen> <entry> <entry> ... <entry> <zlend>
    unsigned int bytes = ZIPLIST_HEADER_SIZE+1;
	//进行空间开启处理,并获取对应的空间指向
    unsigned char *zl = zmalloc(bytes);
	//初始化压缩列表的字节总数
    ZIPLIST_BYTES(zl) = intrev32ifbe(bytes);
	//初始化压缩列表的尾部节点的位置偏移
    ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(ZIPLIST_HEADER_SIZE);
	//初始化压缩列表的元素个数
    ZIPLIST_LENGTH(zl) = 0;
	//初始化结束标识中的结束标记
    zl[bytes-1] = ZIP_END;
	//返回创建的压缩列表的指针指向
    return zl;
}

/* 对给定的压缩列表进行扩容到指定字节空间的处理
 * Resize the ziplist. 
 */
unsigned char *ziplistResize(unsigned char *zl, unsigned int len) {
    //进行空间重分配操作处理
    zl = zrealloc(zl,len);
	//重新设置压缩列表所占据的字节数量
    ZIPLIST_BYTES(zl) = intrev32ifbe(len);
	//设置结束标识
    zl[len-1] = ZIP_END;
	//返回重新分配空间的压缩列表指针指向
    return zl;
}

/* 在给定的节点开始向后检测,后置节点中表示前置节点的字节数量的字节空间大小是否还能够表示对应的前置节点的字节大小
 *    这个地方一开始没看明白的地方是  自己想着 空间不够或者空间充足都需要进行扩容或者移动元素的处理,但是这个地方作者没有这个做,只有当空间不足时才开始进行扩容并移动元素处理
 *    当空间充足时,需要重置大空间中的数据,使用大空间表示小长度
 * When an entry is inserted, we need to set the prevlen field of the next
 * entry to equal the length of the inserted entry. It can occur that this
 * length cannot be encoded in 1 byte and the next entry needs to be grow
 * a bit larger to hold the 5-byte encoded prevlen. This can be done for free,
 * because this only happens when an entry is already being inserted (which
 * causes a realloc and memmove). However, encoding the prevlen may require
 * that this entry is grown as well. This effect may cascade throughout
 * the ziplist when there are consecutive entries with a size close to
 * ZIP_BIG_PREVLEN, so we need to check that the prevlen can be encoded in
 * every consecutive entry.
 *
 * 当将一个新节点添加到某个节点之前的时候，
 * 如果原节点的 header 空间不足以保存新节点的长度，
 * 那么就需要对原节点的 header 空间进行扩展（从 1 字节扩展到 5 字节）。
 *
 * 但是，当对原节点进行扩展之后，原节点的下一个节点的 prevlen 可能出现空间不足，
 * 这种情况在多个连续节点的长度都接近 ZIP_BIGLEN 时可能发生。
 *
 * 这个函数就用于检查并修复后续节点的空间问题。
 * Note that this effect can also happen in reverse, where the bytes required
 * to encode the prevlen field can shrink. This effect is deliberately ignored,
 * because it can cause a "flapping" effect where a chain prevlen fields is
 * first grown and then shrunk again after consecutive inserts. Rather, the
 * field is allowed to stay larger than necessary, because a large prevlen
 * field implies the ziplist is holding large entries anyway.
 * 反过来说，
 * 因为节点的长度变小而引起的连续缩小也是可能出现的，
 * 不过，为了避免扩展-缩小-扩展-缩小这样的情况反复出现（flapping，抖动），
 * 我们不处理这种情况，而是任由 prevlen 比所需的长度更长。
 *
 * The pointer "p" points to the first entry that does NOT need to be
 * updated, i.e. consecutive fields MAY need an update. */
unsigned char *__ziplistCascadeUpdate(unsigned char *zl, unsigned char *p) {
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), rawlen, rawlensize;
    size_t offset, noffset, extra;
    unsigned char *np;
    zlentry cur, next;
	//循环遍历后续需要处理的节点---->处理因前置节点的字节数量表示的字节数的变化导致的后置节点的节点头部中对应内容的变化
    while (p[0] != ZIP_END) {
		//获取当前指向对应的节点的结构信息
        zipEntry(p, &cur);
		//获取当前节点的总的字节数量
        rawlen = cur.headersize + cur.len;
	    //获取表示本字节数量需要的空间字节数
        rawlensize = zipStorePrevEntryLength(NULL,rawlen);

        /* Abort if there is no next entry. */
        if (p[rawlen] == ZIP_END) 
			break;
		//获取下一个对应的节点的结构信息
        zipEntry(p+rawlen, &next);

        /* Abort when "prevlen" has not changed. */
		//如果对应的字符串长度相等,直接就退出了------>这个地方的判断成功的可能性很低
        if (next.prevrawlen == rawlen) 
			break;
		//这个地方需要判断空间是否够用------->不够用只能进行分配处理,如果够用可以用大空间放置小数据
        if (next.prevrawlensize < rawlensize) {
            /* The "prevlen" field of "next" needs more bytes to hold the raw length of "cur". */
		    //记录对应的偏移位置
            offset = p-zl;
			//计算需要的额外的空间数量--->即对应的节点头部中的前置节点字符数量表示字节空间不足的数量
            extra = rawlensize-next.prevrawlensize;
		    //进行空间扩容操作处理
            zl = ziplistResize(zl,curlen+extra);
			//获取新的节点位置指向
            p = zl+offset;

            /* Current pointer and offset for next element. */
			//获取下一个节点的位置指向
            np = p+rawlen;
			//计算偏移位置
            noffset = np-zl;

            /* Update tail offset when next element is not the tail element. */
			//检测是否到达了尾节点----->不是需要更新尾节点指向
            if ((zl+intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))) != np) {
				//更新尾节点的位置指向
                ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+extra);
            }

            /* Move the tail to the back. */
			//进行元素位置移动操作处理
            memmove(np+rawlensize, np+next.prevrawlensize, curlen-noffset-next.prevrawlensize-1);
			//在预留出来的空间中设置表示前置节点占据空间字节数所对应的信息
            zipStorePrevEntryLength(np,rawlen);

            /* Advance the cursor */
			//设置下一个需要遍历的节点
            p += rawlen;
			//增加压缩列表的总的字节数
            curlen += extra;
        } else {
            if (next.prevrawlensize > rawlensize) {
                /* This would result in shrinking, which we want to avoid. So, set "rawlen" in the available bytes. */
			    //这里使用了一个多字节空间大小来表示一个小的字节长度的处理----->即完成对多字节空间中的数据的更改操作处理
                zipStorePrevEntryLengthLarge(p+rawlen,rawlen);
            } else {
                //感觉这个地方不进行重新设置也是可以的======>本身就是等于对应的数值
                zipStorePrevEntryLength(p+rawlen,rawlen);
            }

            /* Stop here, as the raw length of "next" has not changed. */
			//跳出循环,即不需要处理后续的节点了
            break;
        }
    }
    return zl;
}

/* 在压缩列表中从指定的位置开始删除指定个数的元素节点
 * Delete "num" entries, starting at "p". Returns pointer to the ziplist. 
 */
unsigned char *__ziplistDelete(unsigned char *zl, unsigned char *p, unsigned int num) {
    unsigned int i, totlen, deleted = 0;
    size_t offset;
    int nextdiff = 0;
    zlentry first, tail;
	//解析获取需要删除的第一个元素节点的结构信息
    zipEntry(p, &first);
	//循环处理 获取实际能够进行删除的元素个数
    for (i = 0; p[0] != ZIP_END && i < num; i++) {
        p += zipRawEntryLength(p);
        deleted++;
    }
	
	/* Bytes taken by the element(s) to delete. */
	//记录需要进行删除元素对应的总的字节数量
    totlen = p-first.p; 
	//检测是否有需要删除的字节数量
    if (totlen > 0) {
		//
        if (p[0] != ZIP_END) {
            /* Storing `prevrawlen` in this entry may increase or decrease the number of bytes required compare to the current `prevrawlen`.
             * There always is room to store this, because it was previously stored by an entry that is now being deleted. */
            //检测是否引起了对应的效应------->在添加或者删除时都有可能引发对应的效应
            nextdiff = zipPrevLenByteDiff(p,first.prevrawlen);

            /* Note that there is always space when p jumps backward: if the new previous entry is large, one of the deleted elements
             * had a 5 bytes prevlen header, so there is for sure at least 5 bytes free and we need just 4. */
            //处理开始移动位置的指向
            p -= nextdiff;
		    //重新给对应的节点设置前置节点的占据的字节数量值
            zipStorePrevEntryLength(p,first.prevrawlen);

            /* Update offset for tail */
			//更新对应的尾部节点的偏移位置值
            ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))-totlen);

            /* When the tail contains more than one entry, we need to take
             * "nextdiff" in account as well. Otherwise, a change in the
             * size of prevlen doesn't have an effect on the *tail* offset. */
            //重新解析p节点对应的节点结构信息
            zipEntry(p, &tail);
			//检测是否是尾节点了
            if (p[tail.headersize+tail.len] != ZIP_END) {
				//重新更新对应的尾节点指向的一个偏移值
                ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+nextdiff);
            }

            /* Move tail to the front of the ziplist */
			//进行后置元素的前移操作处理
            memmove(first.p,p,intrev32ifbe(ZIPLIST_BYTES(zl))-(p-zl)-1);
        } else {
            /* The entire tail was deleted. No need to move memory. */
		    //重新设置尾节点的位置指向------>即这个地方已经删除到了尾节点的位置上了
            ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe((first.p-zl)-first.prevrawlen);
        }

        /* Resize and update length */
		//计算删除元素之间的偏移值
        offset = first.p-zl;
		//进行压缩列表的尺寸变化操作处理
        zl = ziplistResize(zl, intrev32ifbe(ZIPLIST_BYTES(zl))-totlen+nextdiff);
		//减少对应的压缩列表的数量
        ZIPLIST_INCR_LENGTH(zl,-deleted);
		//重新获取对应的节点指针指向
        p = zl+offset;

        /* When nextdiff != 0, the raw length of the next entry has changed, so we need to cascade the update throughout the ziplist */
		//最后处理是否引发对应的效应
        if (nextdiff != 0)
			//从指定的节点位置开始检测并处理对应的效应
            zl = __ziplistCascadeUpdate(zl,p);
    }
    return zl;
}

/* 根据给定的元素节点的插入位置进行数据插入操作处理
 * Insert item at "p". 
 */
unsigned char *__ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    //获取当前压缩列表的总字节数
    size_t curlen = intrev32ifbe(ZIPLIST_BYTES(zl)), reqlen;
    unsigned int prevlensize, prevlen = 0;
    size_t offset;
    int nextdiff = 0;
    unsigned char encoding = 0;
    long long value = 123456789; /* initialized to avoid warning. Using a value that is easy to see if for some reason we use it uninitialized. */
    zlentry tail;

    /* Find out prevlen for the entry that is inserted. */
	//首先检测插入位置是否处于结束标记处
    if (p[0] != ZIP_END) {
		//解析对应的前置节点占据字节数量和此字节数量需要几个字节来表示
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
    } else {
        //处于结束位置,获取最后一个节点的位置指向
        unsigned char *ptail = ZIPLIST_ENTRY_TAIL(zl);
		//检测获取到的最后一个节点位置是否处于标识位置,即压缩列表中尚未有数据
        if (ptail[0] != ZIP_END) {
			//解析本尾部节点占据的总的字节数量
            prevlen = zipRawEntryLength(ptail);
        }
    }

    /* See if the entry can be encoded */
	//尝试检测给定的需要插入的字符串是否可以进行整数类型编码操作处理-------->此处就是为了获取需要存储对应的字符串需要多少个字节来进行存储处理
    if (zipTryEncoding(s,slen,&value,&encoding)) {
        /* 'encoding' is set to the appropriate integer encoding */
	    //获取编码整数需要占据的字节数量----->即将对应的字符串转换成对应的整数类型后,整数类型需要几个字节空间来进行存储
        reqlen = zipIntSize(encoding);
    } else {
        /* 'encoding' is untouched, however zipStoreEntryEncoding will use the string length to figure out how to encode it. */
	    //如果不能进行转换处理操作 那么字符串的长度就是当前需要存储的真实长度
        reqlen = slen;
    }
    /* We need space for both the length of the previous entry and the length of the payload. */
	//计算为了表示前一个节点字节数量需要使用几个字节来进行表示
    reqlen += zipStorePrevEntryLength(NULL,prevlen);
	//计算为了表示本节点中数据部分的字节数量需要使用几个字节来进行表示---------->这里计算完成 相当已经拿到了需要插入的新节点的元素总共需要的字节数量
    reqlen += zipStoreEntryEncoding(NULL,encoding,slen);

    /* When the insert position is not equal to the tail, we need to make sure that the next entry can hold this entry's length in its prevlen field. */
    int forcelarge = 0;
	//检测需要插入的元素节点占据的空间大小是否引起了原始本位置上节点表示前置节点空间字节数的变化
	//nextdiff保存新旧编码的插值，如果大于0，说明要对p指向节点的header进行扩展
    nextdiff = (p[0] != ZIP_END) ? zipPrevLenByteDiff(p,reqlen) : 0;



	
	//此处的逻辑需要待进一步分析
    if (nextdiff == -4 && reqlen < 4) {
        nextdiff = 0;
        forcelarge = 1;
    }




    /* Store offset because a realloc may change the address of zl. */
	//记录对应的偏移量,目的是后期要进行扩容处理了,指针可能会变化了
    offset = p-zl;
	//进行扩容操作处理
	//------------------->这个地方有可能 +4或者-4个空间 +4说明原先的节点中表示前置节点空间字节数不足 需要额外增加4个空间 -4 说明空间过量 即可以使用原始表示前置节点字节空间中的4个空间位置
    zl = ziplistResize(zl,curlen+reqlen+nextdiff);
	//获取到对应的新的插入节点的位置
    p = zl+offset;

    /* Apply memory move when necessary and update tail offset. */
	//检测插入位置是否是在结束标识位置----->如果是 就不需要考虑后置节点的移动处理了
    if (p[0] != ZIP_END) {
        /* Subtract one because of the ZIP_END bytes */
	    //进行元素后移处理------->这个地方特别需要注意nextdiff的正负问题  ---->nextdiff为正,原始的节点空间不够 需要多移动对应的空间 nextdiff为负 说明空间超量 少移动空间 将对应的空间留给新插入的元素
        memmove(p+reqlen,p-nextdiff,curlen-offset-1+nextdiff);

        /* Encode this entry's raw length in the next entry. */
        if (forcelarge)
			//
            zipStorePrevEntryLengthLarge(p+reqlen,reqlen);
        else
			//重新设定对应的节点上表示前置节点字节数量的值
            zipStorePrevEntryLength(p+reqlen,reqlen);

        /* Update offset for tail */
		//重新设置尾部节点的位置指向
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+reqlen);

        /* When the tail contains more than one entry, we need to take
         * "nextdiff" in account as well. Otherwise, a change in the
         * size of prevlen doesn't have an effect on the *tail* offset. */
        //
        zipEntry(p+reqlen, &tail);
		//
        if (p[reqlen+tail.headersize+tail.len] != ZIP_END) {
            ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl))+nextdiff);
        }
    } else {
        /* This element will be the new tail. */
	    //直接设置压缩列表新的尾部节点的位置指向
        ZIPLIST_TAIL_OFFSET(zl) = intrev32ifbe(p-zl);
    }

    /* When nextdiff != 0, the raw length of the next entry has changed, so we need to cascade the update throughout the ziplist */
	//检测插入元素节点占据的字节数量是否引起了后续节点表示前置节点的空间变化
    if (nextdiff != 0) {
		//记录偏移量
        offset = p-zl;
		//进行处理导致后续节点的空间变化问题
        zl = __ziplistCascadeUpdate(zl,p+reqlen);
	    //重新拿到对应的节点位置指向
        p = zl+offset;
    }

    /* Write the entry */
	//此处真正进行将新的节点数据写入到对应位置上
	//首先写入前置节点对应长度的信息
    p += zipStorePrevEntryLength(p,prevlen);
	//然后写入需要插入节点的数据长度信息
    p += zipStoreEntryEncoding(p,encoding,slen);
	//根据编码方式确定是否是字符串类型
    if (ZIP_IS_STR(encoding)) {
		//写入字符串数据
        memcpy(p,s,slen);
    } else {
        //存储对应的整数数据
        zipSaveInteger(p,value,encoding);
    }
	//设置压缩列表插入元素后元素个数进行增加处理
    ZIPLIST_INCR_LENGTH(zl,1);
	//返回对应的压缩列表结构的位置指向
    return zl;
}

/* 将第二个压缩列表插入到第一个压缩列表的后面
 * Merge ziplists 'first' and 'second' by appending 'second' to 'first'.
 *
 * NOTE: The larger ziplist is reallocated to contain the new merged ziplist.
 * Either 'first' or 'second' can be used for the result.  The parameter not
 * used will be free'd and set to NULL.
 *
 * After calling this function, the input parameters are no longer valid since
 * they are changed and free'd in-place.
 *
 * The result ziplist is the contents of 'first' followed by 'second'.
 *
 * On failure: returns NULL if the merge is impossible.
 * On success: returns the merged ziplist (which is expanded version of either
 * 'first' or 'second', also frees the other unused input ziplist, and sets the
 * input ziplist argument equal to newly reallocated ziplist return value.
 */
unsigned char *ziplistMerge(unsigned char **first, unsigned char **second) {
    /* If any params are null, we can't merge, so NULL. */
    //首先检测给定的两个压缩列表参数是否合法
    if (first == NULL || *first == NULL || second == NULL || *second == NULL)
        return NULL;

    /* Can't merge same list into itself. */
	//检测两个压缩列表是否是同一个压缩列表
    if (*first == *second)
        return NULL;
	//获取第一个压缩列表的总字节数
    size_t first_bytes = intrev32ifbe(ZIPLIST_BYTES(*first));
	//获取第一个压缩列表的总元素个数
    size_t first_len = intrev16ifbe(ZIPLIST_LENGTH(*first));

	//获取第二个压缩列表的总字节数
    size_t second_bytes = intrev32ifbe(ZIPLIST_BYTES(*second));
	//获取第二个压缩列表的总元素个数
    size_t second_len = intrev16ifbe(ZIPLIST_LENGTH(*second));

    int append;
    unsigned char *source, *target;
    size_t target_bytes, source_bytes;
    /* Pick the largest ziplist so we can resize easily in-place.
     * We must also track if we are now appending or prepending to the target ziplist. */
    //处理将短的添加到长元素列表后面的处理------->这个地方理解错了  不是短的要插入到长的后面  其实插入的顺序已经确认了 这个地方只是想尽可能的少移动元素的处理
    if (first_len >= second_len) {
        /* retain first, append second to first. */
        target = *first;
        target_bytes = first_bytes;
        source = *second;
        source_bytes = second_bytes;
        append = 1;
    } else {
        /* else, retain second, prepend first to second. */
        target = *second;
        target_bytes = second_bytes;
        source = *first;
        source_bytes = first_bytes;
        append = 0;
    }

    /* Calculate final bytes (subtract one pair of metadata) */
	//计算新的压缩列表对应的总字节数
    size_t zlbytes = first_bytes + second_bytes - ZIPLIST_HEADER_SIZE - ZIPLIST_END_SIZE;
	//计算新的压缩列表对应的总的元素个数
    size_t zllength = first_len + second_len;

    /* Combined zl length should be limited within UINT16_MAX */
	//处理总元素个数是否超过对应的16位最大整数的处理
    zllength = zllength < UINT16_MAX ? zllength : UINT16_MAX;

    /* Save offset positions before we start ripping memory apart. */
	//分别获取对应的尾节点偏移量
    size_t first_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*first));
    size_t second_offset = intrev32ifbe(ZIPLIST_TAIL_OFFSET(*second));

    /* Extend target to new zlbytes then append or prepend source. */
	//给目标压缩列表进行重新分配对应总大小空间的处理
    target = zrealloc(target, zlbytes);
    if (append) {
        /* append == appending to target */
        /* Copy source after target (copying over original [END]):
         *   [TARGET - END, SOURCE - HEADER] */
        //进行元素拷贝操作处理
        memcpy(target + target_bytes - ZIPLIST_END_SIZE,source + ZIPLIST_HEADER_SIZE,source_bytes - ZIPLIST_HEADER_SIZE);
    } else {
        /* !append == prepending to target */
        /* Move target *contents* exactly size of (source - [END]),
         * then copy source into vacataed space (source - [END]):
         *   [SOURCE - END, TARGET - HEADER] */
        //这个地方相当于将第二个压缩列表中的数据整体移动到后端---->目的给第一个压缩列表插入留出空间
        memmove(target + source_bytes - ZIPLIST_END_SIZE,target + ZIPLIST_HEADER_SIZE,target_bytes - ZIPLIST_HEADER_SIZE);
		//将第一个压缩列表中的元素直接整体插入到头部区域
        memcpy(target, source, source_bytes - ZIPLIST_END_SIZE);
    }

    /* Update header metadata. */
	//更新压缩列表的结构信息参数 总字节数 总元素数
    ZIPLIST_BYTES(target) = intrev32ifbe(zlbytes);
    ZIPLIST_LENGTH(target) = intrev16ifbe(zllength);
    /* New tail offset is:
     *   + N bytes of first ziplist
     *   - 1 byte for [END] of first ziplist
     *   + M bytes for the offset of the original tail of the second ziplist
     *   - J bytes for HEADER because second_offset keeps no header. */
    //计算对应的尾部节点的位置指向问题
    ZIPLIST_TAIL_OFFSET(target) = intrev32ifbe((first_bytes - ZIPLIST_END_SIZE) + (second_offset - ZIPLIST_HEADER_SIZE));

    /* __ziplistCascadeUpdate just fixes the prev length values until it finds a
     * correct prev length value (then it assumes the rest of the list is okay).
     * We tell CascadeUpdate to start at the first ziplist's tail element to fix the merge seam. */
    //从新插入的多个元素的位置开始检测是否引发对应的效应--------------------->处理引发的效应
    target = __ziplistCascadeUpdate(target, target+first_offset);

    /* Now free and NULL out what we didn't realloc */
	//根据append值进一步确定需要释放的压缩列表的指针指向
    if (append) {
        zfree(*second);
        *second = NULL;
        *first = target;
    } else {
        zfree(*first);
        *first = NULL;
        *second = target;
    }
	//返回对应的目标压缩列表
    return target;
}

/*在压缩列表的头或者尾进行数据的插入操作处理*/
unsigned char *ziplistPush(unsigned char *zl, unsigned char *s, unsigned int slen, int where) {
    unsigned char *p;
	//根据where参数获取插入元素位置的指针指向
    p = (where == ZIPLIST_HEAD) ? ZIPLIST_ENTRY_HEAD(zl) : ZIPLIST_ENTRY_END(zl);
	//进行数据插入操作处理
    return __ziplistInsert(zl,p,s,slen);
}

/* 在压缩列表中获取给定索引位置节点元素的指向位置
 * Returns an offset to use for iterating with ziplistNext. When the given
 * index is negative, the list is traversed back to front. When the list
 * doesn't contain an element at the provided index, NULL is returned. 
 */
unsigned char *ziplistIndex(unsigned char *zl, int index) {
    unsigned char *p;
    unsigned int prevlensize, prevlen = 0;
	//检测给定索引是否为负数的情况
    if (index < 0) {
		//计算对应的索引
        index = (-index)-1;
		//获取对应的最后节点元素指向
        p = ZIPLIST_ENTRY_TAIL(zl);
	    //检测是否有对应的元素节点
        if (p[0] != ZIP_END) {
			//解析对应此节点的前置节点的信息
            ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
			//循环向前移动节点------>注意这个地方有一个判断条件为前置节点的长度为0 即到达了开头位置
            while (prevlen > 0 && index--) {
				//向前移动对应的字节数量
                p -= prevlen;
				//进行解析新的前置节点
                ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
            }
        }
    } else {
        //获取压缩列表的起始节点元素位置
        p = ZIPLIST_ENTRY_HEAD(zl);
		//循环遍历,直到对应的索引位置上的节点元素指向
        while (p[0] != ZIP_END && index--) {
			//获取下一个节点元素的位置指向
            p += zipRawEntryLength(p);
        }
    }
	//通过检测对应的指向位置上是否是结束标记来返回对应索引位置上元素节点的指向位置
    return (p[0] == ZIP_END || index > 0) ? NULL : p;
}

/* 获取压缩列表中指定节点元素的下一个节点元素的位置指向
 * Return pointer to next entry in ziplist.
 *
 * zl is the pointer to the ziplist
 * p is the pointer to the current element
 *
 * The element after 'p' is returned, otherwise NULL if we are at the end. 
 */
unsigned char *ziplistNext(unsigned char *zl, unsigned char *p) {
    ((void) zl);

    /* "p" could be equal to ZIP_END, caused by ziplistDelete, and we should return NULL. Otherwise, we should return NULL when the *next* element is ZIP_END (there is no next entry). */
	//首先检测给定的节点是否处于结束标识位置
    if (p[0] == ZIP_END) {
		//直接返回对应的空对象
        return NULL;
    }
	//元素节点向后移动本元素节点字节长度个位置,即移动到了下一个节点位置
    p += zipRawEntryLength(p);
	//检测是否到达了结束标识位置
    if (p[0] == ZIP_END) {
		//直接返回对应的空对象
        return NULL;
    }
	//返回对应的找到的下一个节点的位置指向
    return p;
}

/* 获取压缩列表中指定节点元素的前一个节点元素的位置指向
 * Return pointer to previous entry in ziplist. 
 */
unsigned char *ziplistPrev(unsigned char *zl, unsigned char *p) {
    unsigned int prevlensize, prevlen = 0;

    /* Iterating backwards from ZIP_END should return the tail. When "p" is equal to the first element of the list, we're already at the head, and should return NULL. */
	//检测当前是否处于结束标识位置
    if (p[0] == ZIP_END) {
		//直接通过压缩列表结构获取对应的元素节点的位置
        p = ZIPLIST_ENTRY_TAIL(zl);
		//进一步检测是否有对应的元素节点
        return (p[0] == ZIP_END) ? NULL : p;
    } else if (p == ZIPLIST_ENTRY_HEAD(zl)) {   //检测是否处于头元素节点位置
        //直接返回空对象
        return NULL;
    } else {
		//解析对应的前置节点的字节数量
        ZIP_DECODE_PREVLEN(p, prevlensize, prevlen);
        assert(prevlen > 0);
		//获取对应的前置节点的位置指向------->这个地方一定能找到对应的元素  ---->前面的判断已经进行了相关的约束处理
        return p-prevlen;
    }
}

/* 获取压缩列表中指定元素节点位置上的数据
 * Get entry pointed to by 'p' and store in either '*sstr' or 'sval' depending
 * on the encoding of the entry. '*sstr' is always set to NULL to be able
 * to find out whether the string pointer or the integer value was set.
 * Return 0 if 'p' points to the end of the ziplist, 1 otherwise. 
 */
unsigned int ziplistGet(unsigned char *p, unsigned char **sstr, unsigned int *slen, long long *sval) {
    zlentry entry;
	//首先检测给定的节点位置是否是结束标记位置或者空对象
    if (p == NULL || p[0] == ZIP_END) 
		//直接返回没有获取到对应数据的标识
		return 0;
	//然后处理对应的存储字符串对应的空间,先进行置空处理
    if (sstr) 
		*sstr = NULL;
	//将对应节点位置转换成对应的节点结构信息实体对象
    zipEntry(p, &entry);
	//检测是否是字符串类型的数据
    if (ZIP_IS_STR(entry.encoding)) {
        if (sstr) {
			//设置字符串数据的长度
            *slen = entry.len;
			//设置字符串的指向位置
            *sstr = p+entry.headersize;
        }
    } else {
        //如果是整数类型的数据,检测调用函数是否分配了对应的存储空间
        if (sval) {
			//获取并设置对应的整数数据
            *sval = zipLoadInteger(p+entry.headersize,entry.encoding);
        }
    }
	//返回获取数据成功标识
    return 1;
}

/* 在压缩列表的指定元素位置上进行数据插入操作处理
 * Insert an entry at "p". 
 */
unsigned char *ziplistInsert(unsigned char *zl, unsigned char *p, unsigned char *s, unsigned int slen) {
    return __ziplistInsert(zl,p,s,slen);
}

/* 在给定的压缩列表中删除单个节点
 * Delete a single entry from the ziplist, pointed to by *p.
 * Also update *p in place, to be able to iterate over the ziplist, while deleting entries. 
 */
unsigned char *ziplistDelete(unsigned char *zl, unsigned char **p) {
    //记录偏移量
    size_t offset = *p-zl;
	//触发删除操作处理
    zl = __ziplistDelete(zl,*p,1);

    /* Store pointer to current element in p, because ziplistDelete will
     * do a realloc which might result in a different "zl"-pointer.
     * When the delete direction is back to front, we might delete the last
     * entry and end up with "p" pointing to ZIP_END, so check this. */
    //记录删除节点后的位置指向
    *p = zl+offset;
    return zl;
}

/* 在给定的压缩列表中从指定位置开始删除指定数目的节点元素
 * Delete a range of entries from the ziplist. 
 */
unsigned char *ziplistDeleteRange(unsigned char *zl, int index, unsigned int num) {
    //首先获取压缩列表中对应索引位置上的元素节点的位置指向
    unsigned char *p = ziplistIndex(zl,index);
	//触发从对应的元素节点开始进行删除指定数量的节点元素
    return (p == NULL) ? zl : __ziplistDelete(zl,p,num);
}

/* 检测压缩列表中指定的节点位置上的数据是否与对应的字符串相同
 * Compare entry pointer to by 'p' with 'sstr' of length 'slen'. 
 * Return 1 if equal.
 */
unsigned int ziplistCompare(unsigned char *p, unsigned char *sstr, unsigned int slen) {
    zlentry entry;
    unsigned char sencoding;
    long long zval, sval;
	//首先检测给定的比较位置是否是结束位置
    if (p[0] == ZIP_END) 
		//直接返回不相同标记
		return 0;
	//获取对应位置上的实体类型表示
    zipEntry(p, &entry);
	//检测是否是字符串类型
    if (ZIP_IS_STR(entry.encoding)) {
        /* Raw compare */
	    //比较对应的字符串长度是否相等
        if (entry.len == slen) {
			//比较对应的字符串内容是否相同
            return memcmp(p+entry.headersize,sstr,slen) == 0;
        } else {
            //长度不同,直接返回不相同标记
            return 0;
        }
    } else {
        /* Try to compare encoded values. Don't compare encoding because different implementations may encoded integers differently. */
	    //尝试将比较的字符串转换成对应的整数形式
        if (zipTryEncoding(sstr,slen,&sval,&sencoding)) {
		  //加载对应节点元素上存储的整数数据
          zval = zipLoadInteger(p+entry.headersize,entry.encoding);
		  //比较两个整数是否相等
          return zval == sval;
        }
    }
	//对应的比较字符串都不能转换成对应的整数表示,直接返回不相同标记
    return 0;
}

/* 检测给定的压缩列表中是否有需要查找的字符串数据
 * Find pointer to the entry equal to the specified entry. Skip 'skip' entries between every comparison. Returns NULL when the field could not be found. 
 */
unsigned char *ziplistFind(unsigned char *p, unsigned char *vstr, unsigned int vlen, unsigned int skip) {
    int skipcnt = 0;
    unsigned char vencoding = 0;
    long long vll = 0;
	//循环遍历压缩列表中的所有节点
    while (p[0] != ZIP_END) {
        unsigned int prevlensize, encoding, lensize, len;
        unsigned char *q;
	    //解析本节点对应的前置节点的信息
        ZIP_DECODE_PREVLENSIZE(p, prevlensize);
		//解析本节点的相关数据信息
        ZIP_DECODE_LENGTH(p + prevlensize, encoding, lensize, len);
		//记录本节点所对应的实际数据所对应的位置
        q = p + prevlensize + lensize;
		//检测本元素节点是否需要进行跳过处理
        if (skipcnt == 0) {
            /* Compare current entry with specified entry */
		    //检测是否是字符串编码方式
            if (ZIP_IS_STR(encoding)) {
				//首先比较字符串的长度是否相等,然后进行比较字符串的内容是否相同
                if (len == vlen && memcmp(q, vstr, vlen) == 0) {
					//满足条件直接返回对应的本节点
                    return p;
                }
            } else {
                /* Find out if the searched field can be encoded. Note that we do it only the first time, once done vencoding is set to non-zero and vll is set to the integer value. */
			    //此处设计的也比较精妙 ----->使用一个标记来标识对应的字符串是否可以转换成对应的整数类型 即这个地方只会执行一次 以后就可以直接使用对应的标识了
                if (vencoding == 0) {
					//进行尝试将对应的字符串编码成对应的整数类型
                    if (!zipTryEncoding(vstr, vlen, &vll, &vencoding)) {
                        /* If the entry can't be encoded we set it to UCHAR_MAX so that we don't retry again the next time. */
					    //不能进行转换处理 即设置一个特殊的标记
                        vencoding = UCHAR_MAX;
                    }
                    /* Must be non-zero by now */
                    assert(vencoding);
                }

                /* Compare current entry with specified entry, do it only if vencoding != UCHAR_MAX because if there is no encoding possible for the field it can't be a valid integer. */
				//检测是否可以进行转换成对应整数类型----->不能转换就直接不需要进行比较了
                if (vencoding != UCHAR_MAX) {
					//获取对应位置上的整数值
                    long long ll = zipLoadInteger(q, encoding);
					//进行值比较操作处理
                    if (ll == vll) {
						//返回对应的节点指向位置
                        return p;
                    }
                }
            }

            /* Reset skip count */
			//重置设置的跳过元素数目
            skipcnt = skip;
        } else {
            /* Skip entry */
		    //进行当前条数自减处理----->即完成需要跳过给定数目的节点元素
            skipcnt--;
        }

        /* Move to next entry */
		//移动到下一个需要遍历的元素节点位置
        p = q + len;
    }
	//循环遍历完所有元素节点也没有找到对应的数据就直接返回空对象
    return NULL;
}

/* 获取给定压缩列表的元素个数
 * Return length of ziplist. 
 */
unsigned int ziplistLen(unsigned char *zl) {
    unsigned int len = 0;
	//检测对应的存储位上的数据是否超过了16位整数的最大值
    if (intrev16ifbe(ZIPLIST_LENGTH(zl)) < UINT16_MAX) {
		//直接进行获取对应的元素个数
        len = intrev16ifbe(ZIPLIST_LENGTH(zl));
    } else {
        //获取第一个节点元素的位置
        unsigned char *p = zl+ZIPLIST_HEADER_SIZE;
		//循环遍历所有的元素,来计算对应的总的元素个数
        while (*p != ZIP_END) {
			//记录下一个元素对应的起始位置
            p += zipRawEntryLength(p);
            len++;
        }

        /* Re-store length if small enough */
		//处理当元素个数超过16位整数,但是一段时间删除了部分元素节点,从而使得元素个数小于了最大16位整数
        if (len < UINT16_MAX) 
			//重新修订对应的节点元素个数
			ZIPLIST_LENGTH(zl) = intrev16ifbe(len);
    }
	//返回节点元素的个数
    return len;
}

/* 获取给定压缩列表占据的总字节数量
 * Return ziplist blob size in bytes. 
 */
size_t ziplistBlobLen(unsigned char *zl) {
    return intrev32ifbe(ZIPLIST_BYTES(zl));
}

//格式化打印压缩列表的信息
void ziplistRepr(unsigned char *zl) {
    unsigned char *p;
    int index = 0;
    zlentry entry;

    printf(
        "{total bytes %d} "
        "{num entries %u}\n"
        "{tail offset %u}\n",
        intrev32ifbe(ZIPLIST_BYTES(zl)),
        intrev16ifbe(ZIPLIST_LENGTH(zl)),
        intrev32ifbe(ZIPLIST_TAIL_OFFSET(zl)));
    p = ZIPLIST_ENTRY_HEAD(zl);
    while(*p != ZIP_END) {
        zipEntry(p, &entry);
        printf(
            "{\n"
                "\taddr 0x%08lx,\n"
                "\tindex %2d,\n"
                "\toffset %5ld,\n"
                "\thdr+entry len: %5u,\n"
                "\thdr len%2u,\n"
                "\tprevrawlen: %5u,\n"
                "\tprevrawlensize: %2u,\n"
                "\tpayload %5u\n",
            (long unsigned)p,
            index,
            (unsigned long) (p-zl),
            entry.headersize+entry.len,
            entry.headersize,
            entry.prevrawlen,
            entry.prevrawlensize,
            entry.len);
        printf("\tbytes: ");
        for (unsigned int i = 0; i < entry.headersize+entry.len; i++) {
            printf("%02x|",p[i]);
        }
        printf("\n");
        p += entry.headersize;
        if (ZIP_IS_STR(entry.encoding)) {
            printf("\t[str]");
            if (entry.len > 40) {
                if (fwrite(p,40,1,stdout) == 0) perror("fwrite");
                printf("...");
            } else {
                if (entry.len &&
                    fwrite(p,entry.len,1,stdout) == 0) perror("fwrite");
            }
        } else {
            printf("\t[int]%lld", (long long) zipLoadInteger(p,entry.encoding));
        }
        printf("\n}\n");
        p += entry.len;
        index++;
    }
    printf("{end}\n\n");
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include "adlist.h"
#include "sds.h"

#define debug(f, ...) { if (DEBUG) printf(f, __VA_ARGS__); }

static unsigned char *createList() {
    unsigned char *zl = ziplistNew();
    zl = ziplistPush(zl, (unsigned char*)"foo", 3, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"quux", 4, ZIPLIST_TAIL);
    zl = ziplistPush(zl, (unsigned char*)"hello", 5, ZIPLIST_HEAD);
    zl = ziplistPush(zl, (unsigned char*)"1024", 4, ZIPLIST_TAIL);
    return zl;
}

static unsigned char *createIntList() {
    unsigned char *zl = ziplistNew();
    char buf[32];

    sprintf(buf, "100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "128000");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "-100");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "4294967296");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_HEAD);
    sprintf(buf, "non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    sprintf(buf, "much much longer non integer");
    zl = ziplistPush(zl, (unsigned char*)buf, strlen(buf), ZIPLIST_TAIL);
    return zl;
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

static void stress(int pos, int num, int maxsize, int dnum) {
    int i,j,k;
    unsigned char *zl;
    char posstr[2][5] = { "HEAD", "TAIL" };
    long long start;
    for (i = 0; i < maxsize; i+=dnum) {
        zl = ziplistNew();
        for (j = 0; j < i; j++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,ZIPLIST_TAIL);
        }

        /* Do num times a push+pop from pos */
        start = usec();
        for (k = 0; k < num; k++) {
            zl = ziplistPush(zl,(unsigned char*)"quux",4,pos);
            zl = ziplistDeleteRange(zl,0,1);
        }
        printf("List size: %8d, bytes: %8d, %dx push+pop (%s): %6lld usec\n",
            i,intrev32ifbe(ZIPLIST_BYTES(zl)),num,posstr[pos],usec()-start);
        zfree(zl);
    }
}

static unsigned char *pop(unsigned char *zl, int where) {
    unsigned char *p, *vstr;
    unsigned int vlen;
    long long vlong;

    p = ziplistIndex(zl,where == ZIPLIST_HEAD ? 0 : -1);
    if (ziplistGet(p,&vstr,&vlen,&vlong)) {
        if (where == ZIPLIST_HEAD)
            printf("Pop head: ");
        else
            printf("Pop tail: ");

        if (vstr) {
            if (vlen && fwrite(vstr,vlen,1,stdout) == 0) perror("fwrite");
        }
        else {
            printf("%lld", vlong);
        }

        printf("\n");
        return ziplistDelete(zl,&p);
    } else {
        printf("ERROR: Could not pop\n");
        exit(1);
    }
}

static int randstring(char *target, unsigned int min, unsigned int max) {
    int p = 0;
    int len = min+rand()%(max-min+1);
    int minval, maxval;
    switch(rand() % 3) {
    case 0:
        minval = 0;
        maxval = 255;
    break;
    case 1:
        minval = 48;
        maxval = 122;
    break;
    case 2:
        minval = 48;
        maxval = 52;
    break;
    default:
        assert(NULL);
    }

    while(p < len)
        target[p++] = minval+rand()%(maxval-minval+1);
    return len;
}

static void verify(unsigned char *zl, zlentry *e) {
    int len = ziplistLen(zl);
    zlentry _e;

    ZIPLIST_ENTRY_ZERO(&_e);

    for (int i = 0; i < len; i++) {
        memset(&e[i], 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, i), &e[i]);

        memset(&_e, 0, sizeof(zlentry));
        zipEntry(ziplistIndex(zl, -len+i), &_e);

        assert(memcmp(&e[i], &_e, sizeof(zlentry)) == 0);
    }
}

int ziplistTest(int argc, char **argv) {
    unsigned char *zl, *p;
    unsigned char *entry;
    unsigned int elen;
    long long value;

    /* If an argument is given, use it as the random seed. */
    if (argc == 2)
        srand(atoi(argv[1]));

    zl = createIntList();
    ziplistRepr(zl);

    zfree(zl);

    zl = createList();
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_HEAD);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zl = pop(zl,ZIPLIST_TAIL);
    ziplistRepr(zl);

    zfree(zl);

    printf("Get element at index 3:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 3);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index 3\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index 4 (out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", p-zl);
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -1 (last element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -1\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -4 (first element):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("ERROR: Could not access index -4\n");
            return 1;
        }
        if (entry) {
            if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            printf("\n");
        } else {
            printf("%lld\n", value);
        }
        printf("\n");
        zfree(zl);
    }

    printf("Get element at index -5 (reverse out of range):\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -5);
        if (p == NULL) {
            printf("No entry\n");
        } else {
            printf("ERROR: Out of range index should return NULL, returned offset: %ld\n", p-zl);
            return 1;
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 0 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 0);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 1 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate list from 2 to end:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 2);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistNext(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate starting out of range:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, 4);
        if (!ziplistGet(p, &entry, &elen, &value)) {
            printf("No entry\n");
        } else {
            printf("ERROR\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Iterate from back to front, deleting all items:\n");
    {
        zl = createList();
        p = ziplistIndex(zl, -1);
        while (ziplistGet(p, &entry, &elen, &value)) {
            printf("Entry: ");
            if (entry) {
                if (elen && fwrite(entry,elen,1,stdout) == 0) perror("fwrite");
            } else {
                printf("%lld", value);
            }
            zl = ziplistDelete(zl,&p);
            p = ziplistPrev(zl,p);
            printf("\n");
        }
        printf("\n");
        zfree(zl);
    }

    printf("Delete inclusive range 0,0:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 0,1:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 0, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete inclusive range 1,2:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 2);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with start index out of range:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 5, 1);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete with num overflow:\n");
    {
        zl = createList();
        zl = ziplistDeleteRange(zl, 1, 5);
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Delete foo while iterating:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        while (ziplistGet(p,&entry,&elen,&value)) {
            if (entry && strncmp("foo",(char*)entry,elen) == 0) {
                printf("Delete foo\n");
                zl = ziplistDelete(zl,&p);
            } else {
                printf("Entry: ");
                if (entry) {
                    if (elen && fwrite(entry,elen,1,stdout) == 0)
                        perror("fwrite");
                } else {
                    printf("%lld",value);
                }
                p = ziplistNext(zl,p);
                printf("\n");
            }
        }
        printf("\n");
        ziplistRepr(zl);
        zfree(zl);
    }

    printf("Regression test for >255 byte strings:\n");
    {
        char v1[257] = {0}, v2[257] = {0};
        memset(v1,'x',256);
        memset(v2,'y',256);
        zl = ziplistNew();
        zl = ziplistPush(zl,(unsigned char*)v1,strlen(v1),ZIPLIST_TAIL);
        zl = ziplistPush(zl,(unsigned char*)v2,strlen(v2),ZIPLIST_TAIL);

        /* Pop values again and compare their value. */
        p = ziplistIndex(zl,0);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v1,(char*)entry,elen) == 0);
        p = ziplistIndex(zl,1);
        assert(ziplistGet(p,&entry,&elen,&value));
        assert(strncmp(v2,(char*)entry,elen) == 0);
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Regression test deleting next to last entries:\n");
    {
        char v[3][257] = {{0}};
        zlentry e[3] = {{.prevrawlensize = 0, .prevrawlen = 0, .lensize = 0,
                         .len = 0, .headersize = 0, .encoding = 0, .p = NULL}};
        size_t i;

        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            memset(v[i], 'a' + i, sizeof(v[0]));
        }

        v[0][256] = '\0';
        v[1][  1] = '\0';
        v[2][256] = '\0';

        zl = ziplistNew();
        for (i = 0; i < (sizeof(v)/sizeof(v[0])); i++) {
            zl = ziplistPush(zl, (unsigned char *) v[i], strlen(v[i]), ZIPLIST_TAIL);
        }

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);
        assert(e[2].prevrawlensize == 1);

        /* Deleting entry 1 will increase `prevrawlensize` for entry 2 */
        unsigned char *p = e[1].p;
        zl = ziplistDelete(zl, &p);

        verify(zl, e);

        assert(e[0].prevrawlensize == 1);
        assert(e[1].prevrawlensize == 5);

        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Create long list and check indices:\n");
    {
        zl = ziplistNew();
        char buf[32];
        int i,len;
        for (i = 0; i < 1000; i++) {
            len = sprintf(buf,"%d",i);
            zl = ziplistPush(zl,(unsigned char*)buf,len,ZIPLIST_TAIL);
        }
        for (i = 0; i < 1000; i++) {
            p = ziplistIndex(zl,i);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(i == value);

            p = ziplistIndex(zl,-i-1);
            assert(ziplistGet(p,NULL,NULL,&value));
            assert(999-i == value);
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Compare strings with ziplist entries:\n");
    {
        zl = createList();
        p = ziplistIndex(zl,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Merge test:\n");
    {
        /* create list gives us: [hello, foo, quux, 1024] */
        zl = createList();
        unsigned char *zl2 = createList();

        unsigned char *zl3 = ziplistNew();
        unsigned char *zl4 = ziplistNew();

        if (ziplistMerge(&zl4, &zl4)) {
            printf("ERROR: Allowed merging of one ziplist into itself.\n");
            return 1;
        }

        /* Merge two empty ziplists, get empty result back. */
        zl4 = ziplistMerge(&zl3, &zl4);
        ziplistRepr(zl4);
        if (ziplistLen(zl4)) {
            printf("ERROR: Merging two empty ziplists created entries.\n");
            return 1;
        }
        zfree(zl4);

        zl2 = ziplistMerge(&zl, &zl2);
        /* merge gives us: [hello, foo, quux, 1024, hello, foo, quux, 1024] */
        ziplistRepr(zl2);

        if (ziplistLen(zl2) != 8) {
            printf("ERROR: Merged length not 8, but: %u\n", ziplistLen(zl2));
            return 1;
        }

        p = ziplistIndex(zl2,0);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,3);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,4);
        if (!ziplistCompare(p,(unsigned char*)"hello",5)) {
            printf("ERROR: not \"hello\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"hella",5)) {
            printf("ERROR: \"hella\"\n");
            return 1;
        }

        p = ziplistIndex(zl2,7);
        if (!ziplistCompare(p,(unsigned char*)"1024",4)) {
            printf("ERROR: not \"1024\"\n");
            return 1;
        }
        if (ziplistCompare(p,(unsigned char*)"1025",4)) {
            printf("ERROR: \"1025\"\n");
            return 1;
        }
        printf("SUCCESS\n\n");
        zfree(zl);
    }

    printf("Stress with random payloads of different encoding:\n");
    {
        int i,j,len,where;
        unsigned char *p;
        char buf[1024];
        int buflen;
        list *ref;
        listNode *refnode;

        /* Hold temp vars from ziplist */
        unsigned char *sstr;
        unsigned int slen;
        long long sval;

        for (i = 0; i < 20000; i++) {
            zl = ziplistNew();
            ref = listCreate();
            listSetFreeMethod(ref,(void (*)(void*))sdsfree);
            len = rand() % 256;

            /* Create lists */
            for (j = 0; j < len; j++) {
                where = (rand() & 1) ? ZIPLIST_HEAD : ZIPLIST_TAIL;
                if (rand() % 2) {
                    buflen = randstring(buf,1,sizeof(buf)-1);
                } else {
                    switch(rand() % 3) {
                    case 0:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) >> 20);
                        break;
                    case 1:
                        buflen = sprintf(buf,"%lld",(0LL + rand()));
                        break;
                    case 2:
                        buflen = sprintf(buf,"%lld",(0LL + rand()) << 20);
                        break;
                    default:
                        assert(NULL);
                    }
                }

                /* Add to ziplist */
                zl = ziplistPush(zl, (unsigned char*)buf, buflen, where);

                /* Add to reference list */
                if (where == ZIPLIST_HEAD) {
                    listAddNodeHead(ref,sdsnewlen(buf, buflen));
                } else if (where == ZIPLIST_TAIL) {
                    listAddNodeTail(ref,sdsnewlen(buf, buflen));
                } else {
                    assert(NULL);
                }
            }

            assert(listLength(ref) == ziplistLen(zl));
            for (j = 0; j < len; j++) {
                /* Naive way to get elements, but similar to the stresser
                 * executed from the Tcl test suite. */
                p = ziplistIndex(zl,j);
                refnode = listIndex(ref,j);

                assert(ziplistGet(p,&sstr,&slen,&sval));
                if (sstr == NULL) {
                    buflen = sprintf(buf,"%lld",sval);
                } else {
                    buflen = slen;
                    memcpy(buf,sstr,buflen);
                    buf[buflen] = '\0';
                }
                assert(memcmp(buf,listNodeValue(refnode),buflen) == 0);
            }
            zfree(zl);
            listRelease(ref);
        }
        printf("SUCCESS\n\n");
    }

    printf("Stress with variable ziplist size:\n");
    {
        stress(ZIPLIST_HEAD,100000,16384,256);
        stress(ZIPLIST_TAIL,100000,16384,256);
    }

    return 0;
}
#endif








