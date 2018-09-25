/*
 * 用于存储整数集合的一种实现方式
 */

#ifndef __INTSET_H
#define __INTSET_H
#include <stdint.h>

/* 整数集合的存储表示结构 */
typedef struct intset {
    //表示当前整数集合的编码方式  即使用字节来表示一个整数
    uint32_t encoding;
	//集合中元素的个数
    uint32_t length;
	//用于真正存储整数集合的数组
    int8_t contents[];
} intset;

/* 整数集合对外提供的处理函数 */
intset *intsetNew(void);
intset *intsetAdd(intset *is, int64_t value, uint8_t *success);
intset *intsetRemove(intset *is, int64_t value, int *success);
uint8_t intsetFind(intset *is, int64_t value);
int64_t intsetRandom(intset *is);
uint8_t intsetGet(intset *is, uint32_t pos, int64_t *value);
uint32_t intsetLen(const intset *is);
size_t intsetBlobLen(intset *is);



#ifdef REDIS_TEST
int intsetTest(int argc, char *argv[]);
#endif

#endif // __INTSET_H













