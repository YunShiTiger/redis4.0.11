/*
 * 用于存储整数集合的一种实现方式
 *    需要注意元素是唯一的  且大小是有顺序的
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "intset.h"
#include "zmalloc.h"
#include "endianconv.h"

/* 对应的字节个数的宏定义
 * Note that these encodings are ordered, so:
 * INTSET_ENC_INT16 < INTSET_ENC_INT32 < INTSET_ENC_INT64.  2字节 < 4字节 < 8字节
 */
#define INTSET_ENC_INT16 (sizeof(int16_t))
#define INTSET_ENC_INT32 (sizeof(int32_t))
#define INTSET_ENC_INT64 (sizeof(int64_t))

/* 获取给定的高字节整数真正需要使用几个字节的空间个数
 * Return the required encoding for the provided value. 
 */
static uint8_t _intsetValueEncoding(int64_t v) {
    if (v < INT32_MIN || v > INT32_MAX)
        return INTSET_ENC_INT64;
    else if (v < INT16_MIN || v > INT16_MAX)
        return INTSET_ENC_INT32;
    else
        return INTSET_ENC_INT16;
}

/* 根据给定的编码方式获取给定整数集合指定索引位置的整数
 * Return the value at pos, given an encoding. 
 */
static int64_t _intsetGetEncoded(intset *is, int pos, uint8_t enc) {
    int64_t v64;
    int32_t v32;
    int16_t v16;
	//主要是根据编码方式将对应的整数集合中的数组指针转换成对应的指针类型,然后获取对应位置上的指定长度的整数
    if (enc == INTSET_ENC_INT64) {
        memcpy(&v64,((int64_t*)is->contents)+pos,sizeof(v64));
        memrev64ifbe(&v64);
        return v64;
    } else if (enc == INTSET_ENC_INT32) {
        memcpy(&v32,((int32_t*)is->contents)+pos,sizeof(v32));
        memrev32ifbe(&v32);
        return v32;
    } else {
        memcpy(&v16,((int16_t*)is->contents)+pos,sizeof(v16));
        memrev16ifbe(&v16);
        return v16;
    }
}

/* 获取给定整数集合指定索引位置上的整数
 * Return the value at pos, using the configured encoding. 
 */
static int64_t _intsetGet(intset *is, int pos) {
    return _intsetGetEncoded(is,pos,intrev32ifbe(is->encoding));
}

/* 在给定整数集合的指定位置设置给定的整数
 * Set the value at pos, using the configured encoding. 
 */
static void _intsetSet(intset *is, int pos, int64_t value) {
    //首先获取整数集合的编码方式
    uint32_t encoding = intrev32ifbe(is->encoding);
	//根据编码方式将整数集合中的数组指向转换为对应的指针类型 然后在对应的索引位置上设置给定的整数
    if (encoding == INTSET_ENC_INT64) {
        ((int64_t*)is->contents)[pos] = value;
        memrev64ifbe(((int64_t*)is->contents)+pos);
    } else if (encoding == INTSET_ENC_INT32) {
        ((int32_t*)is->contents)[pos] = value;
        memrev32ifbe(((int32_t*)is->contents)+pos);
    } else {
        ((int16_t*)is->contents)[pos] = value;
        memrev16ifbe(((int16_t*)is->contents)+pos);
    }
}

/* 创建给定的整数集合存储结构
 * Create an empty intset. 
 */
intset *intsetNew(void) {
    //分配对应结构的空间
    intset *is = zmalloc(sizeof(intset));
	//初始设置编码方式为存储2字节整数
    is->encoding = intrev32ifbe(INTSET_ENC_INT16);
	//设置集合元素个数为0个
    is->length = 0;
	//返回整数集合存储结构的指向
    return is;
}

/* 给整数集合进行扩容处理
 * Resize the intset 
 */
static intset *intsetResize(intset *is, uint32_t len) {
    //首先计算出需要的总的字节空间个数
    uint32_t size = len*intrev32ifbe(is->encoding);
	//进行空间重新分配处理            注意这个地方进行空间分配处理是结构需要空间个数+数组元素需要空间个数
    is = zrealloc(is,sizeof(intset)+size);
	//返回扩容之后的整数集合的指针指向
    return is;
}

/* 在整数集合中查找给定整数是否在整数集合中,如果存在获取其对应的索引位置,如果不存在就获取对应的本元素的插入位置
 *   此处使用的是二分查找法 因为整数集合中的元素是唯一的且按照顺序进行排序的
 * Search for the position of "value". Return 1 when the value was found and
 * sets "pos" to the position of the value within the intset. Return 0 when
 * the value is not present in the intset and sets "pos" to the position
 * where "value" can be inserted. 
 */
static uint8_t intsetSearch(intset *is, int64_t value, uint32_t *pos) {
    //首先初始化 最小和最大 以及中间位置的值
    int min = 0, max = intrev32ifbe(is->length)-1, mid = -1;
    int64_t cur = -1;

    /* The value can never be found when the set is empty */
	//特殊处理整数集合中没有元素或者给定的整数大于或者小于整数集合中的所有元素的特殊情况
    if (intrev32ifbe(is->length) == 0) {
        if (pos) 
			//设置需要插入的位置为0索引位置
			*pos = 0;
		//返回本整数集合中不存在给定整数的标识
        return 0;
    } else {
        /* Check for the case where we know we cannot find the value, but do know the insert position. */
	    //检测给定的元素是否大于整数集合中的所有元素
        if (value > _intsetGet(is,intrev32ifbe(is->length)-1)) {
            if (pos) 
				//设置插入位置为当前整数集合的元素个数 即索引位置就是 最后一个元素的后面
				*pos = intrev32ifbe(is->length);
			//返回本整数集合中不存在给定整数的标识
            return 0;
        } else if (value < _intsetGet(is,0)) {
            if (pos) 
				//设置插入位置为开始位置
				*pos = 0;
			//返回本整数集合中不存在给定整数的标识
            return 0;
        }
    }

	//排除上面的特殊情况下 进行二分查找元素是否在集合中,同时获取对应的索引位置
    while(max >= min) {
		//获取最新的中间位置
        mid = ((unsigned int)min + (unsigned int)max) >> 1;
		//获取对应的中间位置的整数
        cur = _intsetGet(is,mid);
	    //分析比较给定的整数与中间位置整数的大小
        if (value > cur) {
			//设置新的最小索引位置
            min = mid+1;
        } else if (value < cur) {
            //设置新的最大索引位置
            max = mid-1;
        } else {
			//此处说明已经找到对应的整数
            break;
        }
    }
	//通过检测当前找到的整数与需要查找的整数的大小来进一步确认整数集合中是否存在本整数
    if (value == cur) {
        if (pos) 
			//设置返回找到的索引位置
			*pos = mid;
		//返回本整数集合中存在给定整数的标识
        return 1;
    } else {
        if (pos) 
			//设置返回能够插入本整数的最小索引位置
			*pos = min;
		//返回本整数集合中不存在给定整数的标识
        return 0;
    }
}

/* 提升整数集合的编码方式同时插入对应的整数----->注意这个地方既然是提升操作处理,那么必然插入的元素一定处于最尾的位置或者处于最头位置上
 * Upgrades the intset to a larger encoding and inserts the given integer. 
 */
static intset *intsetUpgradeAndAdd(intset *is, int64_t value) {
    //获取整数集合当前的编码字节数
    uint8_t curenc = intrev32ifbe(is->encoding);
	//获取需要升级到的编码字节数
    uint8_t newenc = _intsetValueEncoding(value);
	//获取整数集合当前的元素个数
    int length = intrev32ifbe(is->length);
	//根据给定整数的正负确定插入位置是在开始位置还是最后位置
    int prepend = value < 0 ? 1 : 0;

    /* First set new encoding and resize */
	//设置整数集合新的编码方式
    is->encoding = intrev32ifbe(newenc);
	//进行整数集合的空间升级并扩容处理
    is = intsetResize(is,intrev32ifbe(is->length)+1);

    /* Upgrade back-to-front so we don't overwrite values.
     * Note that the "prepend" variable is used to make sure we have an empty
     * space at either the beginning or the end of the intset. */
    //循环处理原始整数集合中的元素占据的空间------>这个地方设置的非常的巧妙 使用一个标记就完成了给对应的位置留出空间方便后期进行插入元素的处理
    while(length--)
        _intsetSet(is,length+prepend,_intsetGetEncoded(is,length,curenc));

    /* Set the value at the beginning or the end. */
    if (prepend)
		//在开始位置插入本整数元素
        _intsetSet(is,0,value);
    else
		//在结束位置插入本整数元素
        _intsetSet(is,intrev32ifbe(is->length),value);
	//设置整数集合的元素个数
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
	//返回新的整数集合的结构指向
    return is;
}

/*
 * 从给定的整数集合的指定位置开始将元素进行后移操作处理
 */
static void intsetMoveTail(intset *is, uint32_t from, uint32_t to) {
    void *src, *dst;
	//计算需要移动元素个数
    uint32_t bytes = intrev32ifbe(is->length)-from;
	//获取整数集合的编码字节数
    uint32_t encoding = intrev32ifbe(is->encoding);
	//根据编码方式将整数集合的数组指针转换成对应的指针类型 并计算需要移动的空间个数
    if (encoding == INTSET_ENC_INT64) {
        src = (int64_t*)is->contents+from;
        dst = (int64_t*)is->contents+to;
        bytes *= sizeof(int64_t);
    } else if (encoding == INTSET_ENC_INT32) {
        src = (int32_t*)is->contents+from;
        dst = (int32_t*)is->contents+to;
        bytes *= sizeof(int32_t);
    } else {
        src = (int16_t*)is->contents+from;
        dst = (int16_t*)is->contents+to;
        bytes *= sizeof(int16_t);
    }
	//真正进行元素的移动处理
    memmove(dst,src,bytes);
}

/* 在给定的整数集合中进行插入给定的整数
 * Insert an integer in the intset 
 */
intset *intsetAdd(intset *is, int64_t value, uint8_t *success) {
    //首先获取给定整数需要的字节编码个数
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;
    if (success) 
		//此处初始化插入元素成功标识位为1 即插入成功
		*success = 1;

    /* Upgrade encoding if necessary. If we need to upgrade, we know that
     * this value should be either appended (if > 0) or prepended (if < 0),
     * because it lies outside the range of existing values. */
    //检测是否进行集合编码方式的提升操作处理
    if (valenc > intrev32ifbe(is->encoding)) {
        /* This always succeeds, so we don't need to curry *success. */
	    //提升整数集合的编码方式并进行插入元素处理
        return intsetUpgradeAndAdd(is,value);
    } else {
        /* Abort if the value is already present in the set.
         * This call will populate "pos" with the right position to insert
         * the value when it cannot be found. */
        //检测给定的整数是否在整数集合中
        if (intsetSearch(is,value,&pos)) {
            if (success) 
				//设置插入失败标记位为0
				*success = 0;
            return is;
        }

		//在没有存在的前提下 进行整数集合的空间扩充处理
        is = intsetResize(is,intrev32ifbe(is->length)+1);
		//检测需要插入的元素位置是否是最后一个位置，  不是就需要进行元素后移操作处理
        if (pos < intrev32ifbe(is->length)) 
			//从指定位置开始进行元素后移操作处理
			intsetMoveTail(is,pos,pos+1);
    }
	//在指定的位置上插入给定的整数
    _intsetSet(is,pos,value);
	//增加整数集合中元素的个数
    is->length = intrev32ifbe(intrev32ifbe(is->length)+1);
	//返回最新的整数集合结构指向
    return is;
}

/* 在给定整数集合中删除给定的整数
 * Delete integer from intset 
 */
intset *intsetRemove(intset *is, int64_t value, int *success) {
    //首先获取给定整数对应的编码字节数
    uint8_t valenc = _intsetValueEncoding(value);
    uint32_t pos;
	//进行初始化是否删除对应元素的成功标识位为0
    if (success) 
		*success = 0;
	//检测给定整数的编码字节数是否小于或者等于整数集合的编码字节数 满足的情况下再进行查找整数是否在整数集合中
    if (valenc <= intrev32ifbe(is->encoding) && intsetSearch(is,value,&pos)) {
		//获取当前集合中元素的个数
        uint32_t len = intrev32ifbe(is->length);

        /* We know we can delete */
	    //设置删除元素成功标识位为1
        if (success) 
			*success = 1;

        /* Overwrite value with tail and update length */
		//检测给定的需要删除的元素位置是否不是最后一个元素的位置                             需要进行元素位置移动处理
        if (pos < (len-1)) 
			intsetMoveTail(is,pos+1,pos);
		//改变整数集合的尺寸
        is = intsetResize(is,len-1);
		//改变整数集合的元素个数
        is->length = intrev32ifbe(len-1);
    }
	//返回整数集合的指向
    return is;
}

/* 检测给定的整数集合中是否有对应的整数
 * Determine whether a value belongs to this set 
 */
uint8_t intsetFind(intset *is, int64_t value) {
    //首先获取给定整数对应的编码字节数
    uint8_t valenc = _intsetValueEncoding(value);
	//检测给定整数的编码字节数是否小于或者等于整数集合的编码字节数 满足的情况下再进行查找整数是否在整数集合中
    return valenc <= intrev32ifbe(is->encoding) && intsetSearch(is,value,NULL);
}

/* 获取给定整数集合中随机位置的一个整数的值
 * Return random member 
 */
int64_t intsetRandom(intset *is) {
    return _intsetGet(is,rand()%intrev32ifbe(is->length));
}

/* 获取给定整数集合中给定位置的整数值
 * Get the value at the given position. When this position is
 * out of range the function returns 0, when in range it returns 1. 
 */
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value) {
    if (pos < intrev32ifbe(is->length)) {
        *value = _intsetGet(is,pos);
        return 1;
    }
    return 0;
}

/* 获取给定整数集合中元素的个数
 * Return intset length
 */
uint32_t intsetLen(const intset *is) {
    return intrev32ifbe(is->length);
}

/* 获取给定整数集合占据的总的字节个数
 * Return intset blob size in bytes. 
 */
size_t intsetBlobLen(intset *is) {
    return sizeof(intset)+intrev32ifbe(is->length)*intrev32ifbe(is->encoding);
}

#ifdef REDIS_TEST
#include <sys/time.h>
#include <time.h>

#if 0
static void intsetRepr(intset *is) {
    for (uint32_t i = 0; i < intrev32ifbe(is->length); i++) {
        printf("%lld\n", (uint64_t)_intsetGet(is,i));
    }
    printf("\n");
}

static void error(char *err) {
    printf("%s\n", err);
    exit(1);
}
#endif

static void ok(void) {
    printf("OK\n");
}

static long long usec(void) {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    return (((long long)tv.tv_sec)*1000000)+tv.tv_usec;
}

#define assert(_e) ((_e)?(void)0:(_assert(#_e,__FILE__,__LINE__),exit(1)))
static void _assert(char *estr, char *file, int line) {
    printf("\n\n=== ASSERTION FAILED ===\n");
    printf("==> %s:%d '%s' is not true\n",file,line,estr);
}

static intset *createSet(int bits, int size) {
    uint64_t mask = (1<<bits)-1;
    uint64_t value;
    intset *is = intsetNew();

    for (int i = 0; i < size; i++) {
        if (bits > 32) {
            value = (rand()*rand()) & mask;
        } else {
            value = rand() & mask;
        }
        is = intsetAdd(is,value,NULL);
    }
    return is;
}

static void checkConsistency(intset *is) {
    for (uint32_t i = 0; i < (intrev32ifbe(is->length)-1); i++) {
        uint32_t encoding = intrev32ifbe(is->encoding);

        if (encoding == INTSET_ENC_INT16) {
            int16_t *i16 = (int16_t*)is->contents;
            assert(i16[i] < i16[i+1]);
        } else if (encoding == INTSET_ENC_INT32) {
            int32_t *i32 = (int32_t*)is->contents;
            assert(i32[i] < i32[i+1]);
        } else {
            int64_t *i64 = (int64_t*)is->contents;
            assert(i64[i] < i64[i+1]);
        }
    }
}

#define UNUSED(x) (void)(x)
int intsetTest(int argc, char **argv) {
    uint8_t success;
    int i;
    intset *is;
    srand(time(NULL));

    UNUSED(argc);
    UNUSED(argv);

    printf("Value encodings: "); {
        assert(_intsetValueEncoding(-32768) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(+32767) == INTSET_ENC_INT16);
        assert(_intsetValueEncoding(-32769) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+32768) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483648) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(+2147483647) == INTSET_ENC_INT32);
        assert(_intsetValueEncoding(-2147483649) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+2147483648) == INTSET_ENC_INT64);
        assert(_intsetValueEncoding(-9223372036854775808ull) ==
                    INTSET_ENC_INT64);
        assert(_intsetValueEncoding(+9223372036854775807ull) ==
                    INTSET_ENC_INT64);
        ok();
    }

    printf("Basic adding: "); {
        is = intsetNew();
        is = intsetAdd(is,5,&success); assert(success);
        is = intsetAdd(is,6,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(success);
        is = intsetAdd(is,4,&success); assert(!success);
        ok();
    }

    printf("Large number of random adds: "); {
        uint32_t inserts = 0;
        is = intsetNew();
        for (i = 0; i < 1024; i++) {
            is = intsetAdd(is,rand()%0x800,&success);
            if (success) inserts++;
        }
        assert(intrev32ifbe(is->length) == inserts);
        checkConsistency(is);
        ok();
    }

    printf("Upgrade from int16 to int32: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,65535));
        checkConsistency(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-65535));
        checkConsistency(is);
        ok();
    }

    printf("Upgrade from int16 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);

        is = intsetNew();
        is = intsetAdd(is,32,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT16);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,32));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
    }

    printf("Upgrade from int32 to int64: "); {
        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,4294967295));
        checkConsistency(is);

        is = intsetNew();
        is = intsetAdd(is,65535,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT32);
        is = intsetAdd(is,-4294967295,NULL);
        assert(intrev32ifbe(is->encoding) == INTSET_ENC_INT64);
        assert(intsetFind(is,65535));
        assert(intsetFind(is,-4294967295));
        checkConsistency(is);
        ok();
    }

    printf("Stress lookups: "); {
        long num = 100000, size = 10000;
        int i, bits = 20;
        long long start;
        is = createSet(bits,size);
        checkConsistency(is);

        start = usec();
        for (i = 0; i < num; i++) intsetSearch(is,rand() % ((1<<bits)-1),NULL);
        printf("%ld lookups, %ld element set, %lldusec\n",
               num,size,usec()-start);
    }

    printf("Stress add+delete: "); {
        int i, v1, v2;
        is = intsetNew();
        for (i = 0; i < 0xffff; i++) {
            v1 = rand() % 0xfff;
            is = intsetAdd(is,v1,NULL);
            assert(intsetFind(is,v1));

            v2 = rand() % 0xfff;
            is = intsetRemove(is,v2,NULL);
            assert(!intsetFind(is,v2));
        }
        checkConsistency(is);
        ok();
    }

    return 0;
}
#endif






