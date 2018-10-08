/* 
 * quicklist.c - A doubly linked list of ziplists
 * 首先回忆下压缩列表的特点：
 *   压缩列表ziplist结构本身就是一个连续的内存块，由表头、若干个entry节点和压缩列表尾部标识符zlend组成，通过一系列编码规则，提高内存的利用率，使用于存储整数和短字符串。
 *   压缩列表ziplist结构的缺点是：每次插入或删除一个元素时，都需要进行频繁的调用realloc()函数进行内存的扩展或减小，然后进行数据”搬移”，甚至可能引发连锁更新，造成严重效率的损失。
 * 
 * quicklist的核心思想其实就是将原始的多数据片段化,减少整体多数据空间的整体移动,进而转化为单片段的少数据量空间的数据移动 从而减少移动空间的开销问题
 *
 * 总结quicklist的特点：
 *   quicklist宏观上是一个双向链表，因此，它具有一个双向链表的有点，进行插入或删除操作时非常方便，虽然复杂度为O(n)，但是不需要内存的复制，提高了效率，而且访问两端元素复杂度为O(1)。
 *   quicklist微观上是一片片entry节点，每一片entry节点内存连续且顺序存储，可以通过二分查找以 log2(n)log2(n) 的复杂度进行定位。
 */
 
#include <string.h> /* for memcpy */
#include "quicklist.h"
#include "zmalloc.h"
#include "ziplist.h"
#include "util.h" /* for ll2string */
#include "lzf.h"

#if defined(REDIS_TEST) || defined(REDIS_TEST_VERBOSE)
/* for printf (debug printing), snprintf (genstr) */
#include <stdio.h> 
#endif

#ifndef REDIS_STATIC
#define REDIS_STATIC static
#endif

/* Optimization levels for size-based filling */
static const size_t optimization_level[] = {4096, 8192, 16384, 32768, 65536};

/* Maximum size in bytes of any multi-element ziplist. Larger values will live in their own isolated ziplists. */
#define SIZE_SAFETY_LIMIT 8192

/* Minimum ziplist size in bytes for attempting compression. */
#define MIN_COMPRESS_BYTES 48

/* Minimum size reduction in bytes to store compressed quicklistNode data. This also prevents us from storing compression if the compression resulted in a larger size than the original data. */
#define MIN_COMPRESS_IMPROVE 8

/* If not verbose testing, remove all debug printing. */
#ifndef REDIS_TEST_VERBOSE
#define D(...)
#else
#define D(...)                                                                 \
    do {                                                                       \
        printf("%s:%s:%d:\t", __FILE__, __FUNCTION__, __LINE__);               \
        printf(__VA_ARGS__);                                                   \
        printf("\n");                                                          \
    } while (0);
#endif

/* 初始化对应的quicklistEntry结构中的数据
 * Simple way to give quicklistEntry structs default values with one call. 
 */
#define initEntry(e)                                                           \
    do {                                                                       \
        (e)->zi = (e)->value = NULL;                                           \
        (e)->longval = -123456789;                                             \
        (e)->quicklist = NULL;                                                 \
        (e)->node = NULL;                                                      \
        (e)->offset = 123456789;                                               \
        (e)->sz = 0;                                                           \
    } while (0)

#if __GNUC__ >= 3
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

/* 创建对应的quicklist结构,并获取对应的空间指向
 * Create a new quicklist. Free with quicklistRelease(). 
 */
quicklist *quicklistCreate(void) {
    struct quicklist *quicklist;
	//为对应的quicklist结构分配对应的空间
    quicklist = zmalloc(sizeof(*quicklist));
    quicklist->head = quicklist->tail = NULL;
    quicklist->len = 0;
    quicklist->count = 0;
    quicklist->compress = 0;
    quicklist->fill = -2;
	//返回对应的quicklist结构的指向
    return quicklist;
}

/*
  compress成员对应的配置：list-compress-depth 0 
    后面的数字有以下含义：
        0  表示不压缩。（默认）
        1  表示quicklist列表的两端各有1个节点不压缩，中间的节点压缩。
        2  表示quicklist列表的两端各有2个节点不压缩，中间的节点压缩。
        3  表示quicklist列表的两端各有3个节点不压缩，中间的节点压缩。
        以此类推，最大为 2的16次方。

*/
#define COMPRESS_MAX (1 << 16)
void quicklistSetCompressDepth(quicklist *quicklist, int compress) {
    if (compress > COMPRESS_MAX) {
        compress = COMPRESS_MAX;
    } else if (compress < 0) {
        compress = 0;
    }
	//设置压缩因子
    quicklist->compress = compress;
}

/*
  fill成员对应的配置：list-max-ziplist-size -2 
    当数字为负数，表示以下含义：
         -1  每个quicklistNode节点的ziplist字节大小不能超过4kb。（建议）
         -2  每个quicklistNode节点的ziplist字节大小不能超过8kb。（默认配置）
         -3  每个quicklistNode节点的ziplist字节大小不能超过16kb。（一般不建议）
         -4  每个quicklistNode节点的ziplist字节大小不能超过32kb。（不建议）
         -5  每个quicklistNode节点的ziplist字节大小不能超过64kb。（正常工作量不建议）
    当数字为正数，表示：ziplist结构所最多包含的entry个数。最大值为 2的15次方。
*/
#define FILL_MAX (1 << 15)
void quicklistSetFill(quicklist *quicklist, int fill) {
    if (fill > FILL_MAX) {
        fill = FILL_MAX;
    } else if (fill < -5) {
        fill = -5;
    }
	//设置填充因子
    quicklist->fill = fill;
}

/*配置指定的压缩因子和填充因子*/
void quicklistSetOptions(quicklist *quicklist, int fill, int depth) {
    quicklistSetFill(quicklist, fill);
    quicklistSetCompressDepth(quicklist, depth);
}

/* 创建指定填充因子和压缩因子的quicklist对象
 * Create a new quicklist with some default parameters. 
 */
quicklist *quicklistNew(int fill, int compress) {
    //创建默认参数的quicklist列表对象
    quicklist *quicklist = quicklistCreate();
	//设置指定的压缩和填充因子
    quicklistSetOptions(quicklist, fill, compress);
    return quicklist;
}

/*创建并初始化quicklist列表结构中的一个节点quicklistNode结构*/
REDIS_STATIC quicklistNode *quicklistCreateNode(void) {
    quicklistNode *node;
	//首先给对应的quicklistNode结构分配对应的空间
    node = zmalloc(sizeof(*node));
	//初始化相关的数据
    node->zl = NULL;
    node->count = 0;
    node->sz = 0;
    node->next = node->prev = NULL;
    node->encoding = QUICKLIST_NODE_ENCODING_RAW;
    node->container = QUICKLIST_NODE_CONTAINER_ZIPLIST;
    node->recompress = 0;
	//返回对应的节点指向
    return node;
}

/* 获取当前quicklist列表结构中总共多少数据元素节点
 * Return cached quicklist count 
 */
unsigned long quicklistCount(const quicklist *ql) { 
	return ql->count; 
}

/* 释放对应的quicklist列表中的数据和对应的空间
 * Free entire quicklist. 
 */
void quicklistRelease(quicklist *quicklist) {
    unsigned long len;
    quicklistNode *current, *next;
	//记录对应的头节点指向
    current = quicklist->head;
	//获取当前quicklist列表中结构元素节点的数量
    len = quicklist->len;
	//循环处理,删除
    while (len--) {
		//记录需要遍历的下一个结构元素节点
        next = current->next;
		//释放对应结构节点中真正实体数据部分的空间
        zfree(current->zl);
	    //减少对应数量的数据元素节点的数量
        quicklist->count -= current->count;
		//释放对应的结构节点的空间
        zfree(current);
		//减少quicklist列表中结构节点的数量
        quicklist->len--;
		//设置下一个需要遍历的结构节点
        current = next;
    }
	//最后释放对应的quicklist列表结构占据的空间
    zfree(quicklist);
}

/* 对给定的节点尝试压缩操作处理
 * Compress the ziplist in 'node' and update encoding details.
 * Returns 1 if ziplist compressed successfully.
 * Returns 0 if compression failed or if ziplist too small to compress. 
 */
REDIS_STATIC int __quicklistCompressNode(quicklistNode *node) {
#ifdef REDIS_TEST
    node->attempted_compress = 1;
#endif

    /* Don't bother compressing small values */
    //检测需要压缩的节点中对应的ziplist的字节数是否比较少----->字节数量少就不进行压缩操作处理了
    if (node->sz < MIN_COMPRESS_BYTES)
		//返回不进行压缩操作处理标识
        return 0;
	//首先分配一个足够大的存储需要压缩节点数据的空间
    quicklistLZF *lzf = zmalloc(sizeof(*lzf) + node->sz);

    /* Cancel if compression fails or doesn't compress small enough */
	//进行压缩数据处理          如果压缩失败或者不能够压缩到足够小就退出了
    if (((lzf->sz = lzf_compress(node->zl, node->sz, lzf->compressed, node->sz)) == 0) || lzf->sz + MIN_COMPRESS_IMPROVE >= node->sz) {
        /* lzf_compress aborts/rejects compression if value not compressable. */
	    //释放分配的空间
        zfree(lzf);
		//返回没有压缩处理的标识
        return 0;
    }
	//压缩之后空间绝对变小了,此处进行空间的重新分配操作处理
    lzf = zrealloc(lzf, sizeof(*lzf) + lzf->sz);
	//释放原始使用ziplist存储数据占据的空间
    zfree(node->zl);
	//给节点设置新的压缩数据之后的结构位置指向
    node->zl = (unsigned char *)lzf;
	//给节点设置进行压缩处理标识
    node->encoding = QUICKLIST_NODE_ENCODING_LZF;
	//设置重新进行压缩处理标记
    node->recompress = 0;
	//返回压缩节点数据成功的标识
    return 1;
}

/* 进行对给定的节点进行压缩操作处理的宏
 * Compress only uncompressed nodes. 
 */
#define quicklistCompressNode(_node)                                           \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_RAW) {     \
            __quicklistCompressNode((_node));                                  \
        }                                                                      \
    } while (0)

/* 对给定的节点进行解压缩操作处理
 * Uncompress the ziplist in 'node' and update encoding details.
 * Returns 1 on successful decode, 0 on failure to decode. 
 */
REDIS_STATIC int __quicklistDecompressNode(quicklistNode *node) {
#ifdef REDIS_TEST
    node->attempted_compress = 0;
#endif
    //提前给对应的ziplist分配对应的空间
    void *decompressed = zmalloc(node->sz);
    //获取对应的压缩数据的节点
    quicklistLZF *lzf = (quicklistLZF *)node->zl;
	//进行解压缩操作处理
    if (lzf_decompress(lzf->compressed, lzf->sz, decompressed, node->sz) == 0) {
        /* Someone requested decompress, but we can't decompress.  Not good. */
	    //解压失败释放已经分配的空间
        zfree(decompressed);
		//返回进行解压缩操作失败的标识
        return 0;
    }
	//释放压缩数据占据的空间位置
    zfree(lzf);
	//将对应的解压缩操作后的节点设置到对应的节点位置指向上
    node->zl = decompressed;
	//设置节点类型为非压缩节点类型
    node->encoding = QUICKLIST_NODE_ENCODING_RAW;
	//返回进行解压缩操作成功标识
    return 1;
}

/* 进行对给定的节点进行解压缩操作处理的宏
 *     注意此处节点的recompress标记并没有进行改变操作处理
 * Decompress only compressed nodes. 
 */
#define quicklistDecompressNode(_node)                                         \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {     \
            __quicklistDecompressNode((_node));                                \
        }                                                                      \
    } while (0)

/* 进行对给定的节点进行解压缩操作处理的宏
 *     注意此处节点的recompress标记进行改变操作处理------->可以通过后面的代码来进一步明确recompress标记的意图
 * Force node to not be immediately re-compresable 
 */
#define quicklistDecompressNodeForUse(_node)                                   \
    do {                                                                       \
        if ((_node) && (_node)->encoding == QUICKLIST_NODE_ENCODING_LZF) {     \
            __quicklistDecompressNode((_node));                                \
            (_node)->recompress = 1;                                           \
        }                                                                      \
    } while (0)

/* 获取给定节点的压缩数据,同时返回对应的未进行压缩前的总字节数
 * Extract the raw LZF data from this quicklistNode.
 * Pointer to LZF data is assigned to '*data'.
 * Return value is the length of compressed LZF data. 
 */
size_t quicklistGetLzf(const quicklistNode *node, void **data) {
    //获取给定节点的压缩数据的位置指向
    quicklistLZF *lzf = (quicklistLZF *)node->zl;
	//记录对应的压缩数据的位置指向
    *data = lzf->compressed;
	//返回数据未压缩前的总字节数
    return lzf->sz;
}



/* 检测给定的节点是否进行了压缩操作处理*/
#define quicklistAllowsCompression(_ql) ((_ql)->compress != 0)

/* 强制给对应的quicklist进行整体的解压缩和处理压缩操作处理 即在给定的范围内的进行解压缩操作处理,同时处理给定节点的压缩操作处理
 * Force 'quicklist' to meet compression guidelines set by compress depth.
 * The only way to guarantee interior nodes get compressed is to iterate
 * to our "interior" compress depth then compress the next node we find.
 * If compress depth is larger than the entire list, we return immediately. 
 */
REDIS_STATIC void __quicklistCompress(const quicklist *quicklist, quicklistNode *node) {
    /* If length is less than our compress depth (from both sides), we can't compress anything. */
    //首先检测是否允许压缩处理 以及是否达到压缩处理的界限值
    if (!quicklistAllowsCompression(quicklist) || quicklist->len < (unsigned int)(quicklist->compress * 2))
        return;

#if 0
    /* Optimized cases for small depth counts */
    if (quicklist->compress == 1) {
        quicklistNode *h = quicklist->head, *t = quicklist->tail;
        quicklistDecompressNode(h);
        quicklistDecompressNode(t);
        if (h != node && t != node)
            quicklistCompressNode(node);
        return;
    } else if (quicklist->compress == 2) {
        quicklistNode *h = quicklist->head, *hn = h->next, *hnn = hn->next;
        quicklistNode *t = quicklist->tail, *tp = t->prev, *tpp = tp->prev;
        quicklistDecompressNode(h);
        quicklistDecompressNode(hn);
        quicklistDecompressNode(t);
        quicklistDecompressNode(tp);
        if (h != node && hn != node && t != node && tp != node) {
            quicklistCompressNode(node);
        }
        if (hnn != t) {
            quicklistCompressNode(hnn);
        }
        if (tpp != h) {
            quicklistCompressNode(tpp);
        }
        return;
    }
#endif

    /* Iterate until we reach compress depth for both sides of the list.a
     * Note: because we do length checks at the *top* of this function, we can skip explicit null checks below. Everything exists. */
    quicklistNode *forward = quicklist->head;
    quicklistNode *reverse = quicklist->tail;
    int depth = 0;
    int in_depth = 0;
	//循环进行检测压缩操作处理
    while (depth++ < quicklist->compress) {
		//尝试进行解压缩操作处理
        quicklistDecompressNode(forward);
        quicklistDecompressNode(reverse);
	    //检测给定的节点是否处于不进行压缩操作处理的范围内
        if (forward == node || reverse == node)
            in_depth = 1;

        if (forward == reverse)
            return;
		//设置下一次进行循环处理的结构节点元素
        forward = forward->next;
        reverse = reverse->prev;
    }

    //检测给定的节点是否在需要进行压缩的范围之内
    if (!in_depth)
		//压缩本节点的数据----->即本节点需要进行压缩操作处理
        quicklistCompressNode(node);

    //此处处理压缩临界点的压缩处理
    if (depth > 2) {
        /* At this point, forward and reverse are one node beyond depth */
        quicklistCompressNode(forward);
        quicklistCompressNode(reverse);
    }
}

/* 对给定的节点进行压缩操作处理 如果以前进行过解压缩的操作那么就只对本节点进行压缩操作处理,否则进行分析整体quicklist结构来处理是否对本节点进行压缩操作处理 */
#define quicklistCompress(_ql, _node)                                          \
    do {                                                                       \
        if ((_node)->recompress)                                               \
            quicklistCompressNode((_node));                                    \
        else                                                                   \
            __quicklistCompress((_ql), (_node));                               \
    } while (0)

/* 检测如果之前使用quicklistDecompressNodeForUse对压缩节点进行解压缩处理时,此处重新对应数据进行压缩操作处理
 * If we previously used quicklistDecompressNodeForUse(), just recompress. 
 */
#define quicklistRecompressOnly(_ql, _node)                                    \
    do {                                                                       \
        if ((_node)->recompress)                                               \
            quicklistCompressNode((_node));                                    \
    } while (0)

/* 在给定的结构节点前或者后插入新的结构节点 
 *    需要注意的:新插入的节点一般都没有进行过压缩操作处理
 * Insert 'new_node' after 'old_node' if 'after' is 1.
 * Insert 'new_node' before 'old_node' if 'after' is 0.
 * Note: 'new_node' is *always* uncompressed, so if we assign it to head or tail, we do not need to uncompress it. 
 */
REDIS_STATIC void __quicklistInsertNode(quicklist *quicklist, quicklistNode *old_node, quicklistNode *new_node, int after) {
    //检测给定的节点前还是后进行插入
    if (after) {
		//设置新结构节点的前置指向
        new_node->prev = old_node;
		//检测给定的老节点是否存在
        if (old_node) {
			//设置新结构节点的后置指向
            new_node->next = old_node->next;
			//检测老节点是否有后置节点
            if (old_node->next)
				//进行老节点的后置节点的前置指向处理
                old_node->next->prev = new_node;
			//设置老节点的后置指向
            old_node->next = new_node;
        }
		//检测尾节点是否指向老节点 即quicklist列表中只有一个节点
        if (quicklist->tail == old_node)
			//设置新的尾节点的位置指向
            quicklist->tail = new_node;
    } else {
        //此处的断链和建链过程与上类似
        new_node->next = old_node;
        if (old_node) {
            new_node->prev = old_node->prev;
            if (old_node->prev)
                old_node->prev->next = new_node;
            old_node->prev = new_node;
        }
        if (quicklist->head == old_node)
            quicklist->head = new_node;
    }
    /* If this insert creates the only element so far, initialize head/tail. */
	//检测原先quicklist列表是否有结构节点元素
    if (quicklist->len == 0) {
		//设置quicklist列表的头和尾都指向新创建的结构节点
        quicklist->head = quicklist->tail = new_node;
    }
	//检测老节点是否存在
    if (old_node)
		//对老节点尝试进行压缩操作处理----------------------->即完成一个新的结构节点的插入操作之后,需要尝试对老的结构节点进行压缩操作处理
        quicklistCompress(quicklist, old_node);
	//添加quicklist列表中的结构节点的数量
    quicklist->len++;
}

/* 封装的在对应的给定的结构节点前面插入新的结构节点
 * Wrappers for node inserting around existing node. 
 */
REDIS_STATIC void _quicklistInsertNodeBefore(quicklist *quicklist, quicklistNode *old_node, quicklistNode *new_node) {
    __quicklistInsertNode(quicklist, old_node, new_node, 0);
}

/* 封装的在对应的给定的结构节点后面插入新的结构节点
 * Wrappers for node inserting around existing node. 
 */
REDIS_STATIC void _quicklistInsertNodeAfter(quicklist *quicklist, quicklistNode *old_node, quicklistNode *new_node) {
    __quicklistInsertNode(quicklist, old_node, new_node, 1);
}

/* 检测给定的字节数量值是否在设定的填充因子对应的范围内 */
REDIS_STATIC int _quicklistNodeSizeMeetsOptimizationRequirement(const size_t sz, const int fill) {
    //如果填充因子大于0 说明是需要按照元素数量来进行判断的
    if (fill >= 0)
		//直接返回不满足范围的标识
        return 0;
	//获取填充因子对应的偏移值----->  即将负数变成对应的整数值
    size_t offset = (-fill) - 1;
	//检测计算的偏移值是否在设置的范围内
    if (offset < (sizeof(optimization_level) / sizeof(*optimization_level))) {
		//获取对应偏移出的字节的最大数量值 并检测与给定的数值的大小
        if (sz <= optimization_level[offset]) {
			//返回长度满足范围的标识
            return 1;
        } else {
            //返回长度不满足范围的标识
            return 0;
        }
    } else {
        //因设定的填充因子不合法导致返回不满足范围的标识
        return 0;
    }
}

//sz是否超过ziplist所规定的安全界限8192字节，1表示安全，0表示不安全                            ------>8kb
#define sizeMeetsSafetyLimit(sz) ((sz) <= SIZE_SAFETY_LIMIT)

/*
 * node节点中ziplist能否插入entry节点中，根据fill和sz判断
 */
REDIS_STATIC int _quicklistNodeAllowInsert(const quicklistNode *node, const int fill, const size_t sz) {
    //首先检测给定的结构节点是否存在
    if (unlikely(!node))
		//不存在,直接返回不允许在给定的结构节点上进行元素的插入操作处理
        return 0;

    int ziplist_overhead;
	//下面进行大体估算出插入本元素节点需要的空间个数
    /* size of previous offset */
    if (sz < 254)
        ziplist_overhead = 1;
    else
        ziplist_overhead = 5;

    /* size of forward offset */
    if (sz < 64)
        ziplist_overhead += 1;
    else if (likely(sz < 16384))
        ziplist_overhead += 2;
    else
        ziplist_overhead += 5;

    /* new_sz overestimates if 'sz' encodes to an integer type */
	//大体计算出插入本数据节点后ziplist对应的总的字节数量
    unsigned int new_sz = node->sz + sz + ziplist_overhead;
	//此处的判断分为 检测字节大小是否超过范围 总元素个数是否超过范围 等检测操作处理
    if (likely(_quicklistNodeSizeMeetsOptimizationRequirement(new_sz, fill)))
		//首先检测计算的字节数量是否在设定的满足范围之内
        return 1;
    else if (!sizeMeetsSafetyLimit(new_sz))
		//检测如果插入后新的总字节数超过了预设的8kb 返回不能进行插入标识------->即一个结构节点中的数据节点占据的空的空间个数不能大于8kb
        return 0;
    else if ((int)node->count < fill)
		//本结构节点中总的数据节点的个数小于设置的预设值,返回对应的可以进行插入的标识
        return 1;
    else
		//不满足上述条件 统一返回不能够进行插入操作处理的标识
        return 0;
}

/* 检测是否可以对给定的两个结构节点进行合并操作处理 */
REDIS_STATIC int _quicklistNodeAllowMerge(const quicklistNode *a, const quicklistNode *b, const int fill) {
    //检测给定的两个结构节点是否为空
    if (!a || !b)
        return 0;

    /* approximate merged ziplist size (- 11 to remove one ziplist header/trailer) */
	//计算合并是需要的总的字节数量----->这个值只是一个大体值
    unsigned int merge_sz = a->sz + b->sz - 11;
	//进行检测是否可以进行合并处理------->这个判断处理和上面函数的处理方式相同
    if (likely(_quicklistNodeSizeMeetsOptimizationRequirement(merge_sz, fill)))
        return 1;
    else if (!sizeMeetsSafetyLimit(merge_sz))
        return 0;
    else if ((int)(a->count + b->count) <= fill)
        return 1;
    else
        return 0;
}

/* 用于更新对应结构节点中记录ziplist字节数量的字段值的宏 */
#define quicklistNodeUpdateSz(node)                                            \
    do {                                                                       \
        (node)->sz = ziplistBlobLen((node)->zl);                               \
    } while (0)

/* 在quicklist列表的头部结构节点上插入一个数据节点  ----->同时数据节点插入到对应的ziplist的头部
 * Add new entry to head node of quicklist.
 *
 * Returns 0 if used existing head.
 * Returns 1 if new head created. 
 */
int quicklistPushHead(quicklist *quicklist, void *value, size_t sz) {
    //用于记录原始的头节点的位置指向
    quicklistNode *orig_head = quicklist->head;
	//
    if (likely(_quicklistNodeAllowInsert(quicklist->head, quicklist->fill, sz))) {
		//在原始的头结构节点上开始插入对应的数据元素----->数据插入的位置在对应的头部位置
        quicklist->head->zl = ziplistPush(quicklist->head->zl, value, sz, ZIPLIST_HEAD);
		//更新对应的结构节点上记录的ziplist的总字节长度
        quicklistNodeUpdateSz(quicklist->head);
    } else {
		//创建对应的新的结构节点
        quicklistNode *node = quicklistCreateNode();
		//在新创建的结构节点上进行插入对应的数据元素----->数据插入的位置在对应的头部位置
        node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);
		//更新对应的结构节点上记录的ziplist的总字节长度
        quicklistNodeUpdateSz(node);
		//将对应的新创建的结构节点链接到原始的头结构节点上,即更新了头结构节点
        _quicklistInsertNodeBefore(quicklist, quicklist->head, node);
    }
	//设置quicklist列表的总的数据元素的个数增加处理
    quicklist->count++;
	//设置quicklist列表中对应的头结构节点的数据元素个数增加处理
    quicklist->head->count++;
	//返回头结构节点是否是新创建结构节点的标识
    return (orig_head != quicklist->head);
}

/* 在quicklist列表的尾部结构节点上插入一个数据节点  ----->同时数据节点插入到对应的ziplist的尾部
 * Add new entry to tail node of quicklist.
 *
 * Returns 0 if used existing tail.
 * Returns 1 if new tail created. 
 */
int quicklistPushTail(quicklist *quicklist, void *value, size_t sz) {
    quicklistNode *orig_tail = quicklist->tail;
    if (likely(_quicklistNodeAllowInsert(quicklist->tail, quicklist->fill, sz))) {
        quicklist->tail->zl = ziplistPush(quicklist->tail->zl, value, sz, ZIPLIST_TAIL);
        quicklistNodeUpdateSz(quicklist->tail);
    } else {
        quicklistNode *node = quicklistCreateNode();
        node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_TAIL);
        quicklistNodeUpdateSz(node);
        _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);
    }
    quicklist->count++;
    quicklist->tail->count++;
    return (orig_tail != quicklist->tail);
}

/* 将给定的ziplist结构数据之间链接到quicklist列表结构的尾节点后
 * Create new node consisting of a pre-formed ziplist.
 * Used for loading RDBs where entire ziplists have been stored to be retrieved later. 
 */
void quicklistAppendZiplist(quicklist *quicklist, unsigned char *zl) {
    //创建对应的结构节点
    quicklistNode *node = quicklistCreateNode();
	//设置结构节点中元素节点的位置指向
    node->zl = zl;
	//设置元素节点中总的元素个数
    node->count = ziplistLen(node->zl);
	//设置元素节点中总占据的空间字节数
    node->sz = ziplistBlobLen(zl);
	//将新创建的结构节点插入到尾部节点后
    _quicklistInsertNodeAfter(quicklist, quicklist->tail, node);
	//更新quicklist列表结构的总的元素数量
    quicklist->count += node->count;
}

/* 循环将一个ziplist中的元素插入到quicklist列表的尾部
 * Append all values of ziplist 'zl' individually into 'quicklist'.
 *
 * This allows us to restore old RDB ziplists into new quicklists
 * with smaller ziplist sizes than the saved RDB ziplist.
 *
 * Returns 'quicklist' argument. Frees passed-in ziplist 'zl' 
 */
quicklist *quicklistAppendValuesFromZiplist(quicklist *quicklist, unsigned char *zl) {
    unsigned char *value;
    unsigned int sz;
    long long longval;
    char longstr[32] = {0};
	//获取ziplist首元素位置指向
    unsigned char *p = ziplistIndex(zl, 0);
	//循环处理ziplist中的数据,将其插入到quicklist列表尾部
    while (ziplistGet(p, &value, &sz, &longval)) {
		//检测在ziplist中获取的元素是否是字符串类型
        if (!value) {
            /* Write the longval as a string so we can re-add it */
		    //将对应的整数类型转化成对应的字符串类型
            sz = ll2string(longstr, sizeof(longstr), longval);
			//获取对应的字符串的指向位置
            value = (unsigned char *)longstr;
        }
		//将对应的元素插入到quicklist列表的尾部
        quicklistPushTail(quicklist, value, sz);
		//获取下一个需要遍历的元素节点位置
        p = ziplistNext(zl, p);
    }
	//释放原始ziplist占据的空间
    zfree(zl);
	//返回对应的quicklist列表指向
    return quicklist;
}

/* 根据给定的压缩参数和填充参数以及存在的ziplist来构建对应的quicklist列表结构
 * Create new (potentially multi-node) quicklist from a single existing ziplist.
 *
 * Returns new quicklist.  Frees passed-in ziplist 'zl'. 
 */
quicklist *quicklistCreateFromZiplist(int fill, int compress, unsigned char *zl) {
    return quicklistAppendValuesFromZiplist(quicklistNew(fill, compress), zl);
}

/* 检测是否有必要删除对应的结构节点                 即结构节点中元素个数为0时就需要进行删除了 */
#define quicklistDeleteIfEmpty(ql, n)                                          \
    do {                                                                       \
        if ((n)->count == 0) {                                                 \
            __quicklistDelNode((ql), (n));                                     \
            (n) = NULL;                                                        \
        }                                                                      \
    } while (0)

/* 将对应的结构节点在quicklist列表中进行移除操作处理 */
REDIS_STATIC void __quicklistDelNode(quicklist *quicklist, quicklistNode *node) {
    //处理后置节点的前置指向
    if (node->next)
        node->next->prev = node->prev;
	//处理前置节点的后置指向
    if (node->prev)
        node->prev->next = node->next;
	//检测是否需要设置新的尾结构节点
    if (node == quicklist->tail) {
        quicklist->tail = node->prev;
    }
	//检测是否需要设置新的头结构节点
    if (node == quicklist->head) {
        quicklist->head = node->next;
    }

    /* If we deleted a node within our compress depth, we now have compressed nodes needing to be decompressed. */
    __quicklistCompress(quicklist, NULL);
	//减少quicklist中记录的数据元素个数
    quicklist->count -= node->count;
	//释放对应的结构节点中所有数据节点占据的空间
    zfree(node->zl);
	//释放对应的结构节点占据的空间
    zfree(node);
	//减少quicklist中结构节点的数量
    quicklist->len--;
}

/* 根据给定的结构节点在对应的结构节点元素节点上的对应的元素的位置删除对应于的此元素数据
 * Delete one entry from list given the node for the entry and a pointer to the entry in the node.
 *
 * Note: quicklistDelIndex() *requires* uncompressed nodes because you already had to get *p from an uncompressed node somewhere.
 *
 * Returns 1 if the entire node was deleted, 0 if node still exists.
 * Also updates in/out param 'p' with the next offset in the ziplist. 
 */
REDIS_STATIC int quicklistDelIndex(quicklist *quicklist, quicklistNode *node, unsigned char **p) {
    //用于记录是否进行删除结构节点的标识
    int gone = 0;
	//删除对应位置上元素节点
    node->zl = ziplistDelete(node->zl, p);
	//元素个数进行自减处理
    node->count--;
	//检测本结构节点上的元素个数总数是否减少为0
    if (node->count == 0) {
		//设置需要进行删除本结构节点的标识
        gone = 1;
		//进行删除本结构节点操作处理
        __quicklistDelNode(quicklist, node);
    } else {
		//更新本结构节点对应元素总的字节数量
        quicklistNodeUpdateSz(node);
    }
	//更新quicklist列表中元素的总个数
    quicklist->count--;
    /* If we deleted the node, the original node is no longer valid */
	//返回是否删除了结构节点的标识
    return gone ? 1 : 0;
}

/* 删除给定元素的节点,如果删除成功需要对应迭代器的参数数据
 * Delete one element represented by 'entry'
 *
 * 'entry' stores enough metadata to delete the proper position in the correct ziplist in the correct quicklist node.
 */
void quicklistDelEntry(quicklistIter *iter, quicklistEntry *entry) {
    quicklistNode *prev = entry->node->prev;
    quicklistNode *next = entry->node->next;
	//删除对应位置上的元素信息
    int deleted_node = quicklistDelIndex((quicklist *)entry->quicklist, entry->node, &entry->zi);

    /* after delete, the zi is now invalid for any future usage. */
	//设置对应迭代器中的指向zp的指向为不可用状态
    iter->zi = NULL;

    /* If current node is deleted, we must update iterator node and offset. */
	//检测是否成功删除对应的元素,重新设置对应的迭代器参数
    if (deleted_node) {
        if (iter->direction == AL_START_HEAD) {
            iter->current = next;
            iter->offset = 0;
        } else if (iter->direction == AL_START_TAIL) {
            iter->current = prev;
            iter->offset = -1;
        }
    }
    /* else if (!deleted_node), no changes needed.
     * we already reset iter->zi above, and the existing iter->offset
     * doesn't move again because:
     *   - [1, 2, 3] => delete offset 1 => [1, 3]: next element still offset 1
     *   - [1, 2, 3] => delete offset 0 => [2, 3]: next element still offset 0
     *  if we deleted the last element at offet N and now
     *  length of this ziplist is N-1, the next call into
     *  quicklistNext() will jump to the next node. */
}

/* 替换给定索引位置上的数据---------------->这个地方自己有一个迷惑,如果替换成的字符数据如果非常长的时候是不是造成ziplist特别的长呢？？？？？？？？？？？？？？？？？？？？
 * Replace quicklist entry at offset 'index' by 'data' with length 'sz'.
 *
 * Returns 1 if replace happened.
 * Returns 0 if replace failed and no changes happened. 
 */
int quicklistReplaceAtIndex(quicklist *quicklist, long index, void *data, int sz) {
    quicklistEntry entry;
	//首先检测给定的索引位置是否有对应的节点数据信息
    if (likely(quicklistIndex(quicklist, index, &entry))) {
        /* quicklistIndex provides an uncompressed node */
	    //删除对应位置上的节点数据
        entry.node->zl = ziplistDelete(entry.node->zl, &entry.zi);
		//将新的数据插入到本结构节点的数据上
        entry.node->zl = ziplistInsert(entry.node->zl, entry.zi, data, sz);
	    //更新结构节点中总的字节数量
        quicklistNodeUpdateSz(entry.node);
		//尝试进行压缩处理
        quicklistCompress(quicklist, entry.node);
		//返回进行替换成功的标识
        return 1;
    } else {
		//返回替换失败的标识
        return 0;
    }
}

/* 对给定的quicklist列表中的两个结构节点进行合并操作处理
 * Given two nodes, try to merge their ziplists.
 *
 * This helps us not have a quicklist with 3 element ziplists if
 * our fill factor can handle much higher levels.
 *
 * Note: 'a' must be to the LEFT of 'b'.
 *
 * After calling this function, both 'a' and 'b' should be considered
 * unusable.  The return value from this function must be used
 * instead of re-using any of the quicklistNode input arguments.
 *
 * Returns the input node picked to merge against or NULL if
 * merging was not possible. 
 */
REDIS_STATIC quicklistNode *_quicklistZiplistMerge(quicklist *quicklist, quicklistNode *a, quicklistNode *b) {
    D("Requested merge (a,b) (%u, %u)", a->count, b->count);

    //尝试对a结构节点进行解压缩操作处理
    quicklistDecompressNode(a);
	//尝试对b结构节点进行解压缩操作处理
    quicklistDecompressNode(b);
	//对两个结构节点中的数据进行合并操作处理
    if ((ziplistMerge(&a->zl, &b->zl))) {
        /* We merged ziplists! Now remove the unused quicklistNode. */
        quicklistNode *keep = NULL, *nokeep = NULL;
		//获取进行合并后保留数据节点的结构节点
        if (!a->zl) {
            nokeep = a;
            keep = b;
        } else if (!b->zl) {
            nokeep = b;
            keep = a;
        }
		//获取合并后元素节点的数量
        keep->count = ziplistLen(keep->zl);
		//更新结构节点中总的字节数
        quicklistNodeUpdateSz(keep);

        nokeep->count = 0;
		//在quicklist中删除对应的结构节点
        __quicklistDelNode(quicklist, nokeep);
		//尝试对保留的结构节点中的数据节点进行压缩操作处理
        quicklistCompress(quicklist, keep);
		//返回保留的结构节点
        return keep;
    } else {
        /* else, the merge returned NULL and nothing changed. */
	    //如果没有进行合并操作处理,直接返回null 标识没有进行改变操作处理
        return NULL;
    }
}

/* 尝试将对应的结构节点向给定的中间结构节点聚集
 * Attempt to merge ziplists within two nodes on either side of 'center'.
 *
 * We attempt to merge:
 *   - (center->prev->prev, center->prev)
 *   - (center->next, center->next->next)
 *   - (center->prev, center)
 *   - (center, center->next)
 */
REDIS_STATIC void _quicklistMergeNodes(quicklist *quicklist, quicklistNode *center) {
    int fill = quicklist->fill;
    quicklistNode *prev, *prev_prev, *next, *next_next, *target;
	//首先进行置空指向
    prev = prev_prev = next = next_next = target = NULL;
	//检测是否有前置节点
    if (center->prev) {
		//设置对应的前置节点指向
        prev = center->prev;
		//检测是否有前置节点的前置节点
        if (center->prev->prev)
			//设置对应的前置节点的前置指向
            prev_prev = center->prev->prev;
    }
	//检测是否有后置节点
    if (center->next) {
		//设置对应的后置节点指向
        next = center->next;
		//检测是否有后置节点的后置节点
        if (center->next->next)
			//设置对应的后置节点的后置指向
            next_next = center->next->next;
    }

    /* Try to merge prev_prev and prev */
	//尝试进行前置节点和前置节点的前置节点进行合并操作处理
    if (_quicklistNodeAllowMerge(prev, prev_prev, fill)) {
		//进行合并操作处理
        _quicklistZiplistMerge(quicklist, prev_prev, prev);
        prev_prev = prev = NULL; /* they could have moved, invalidate them. */
    }

    /* Try to merge next and next_next */
	//尝试进行后置节点和后置节点的后置节点进行合并操作处理
    if (_quicklistNodeAllowMerge(next, next_next, fill)) {
		//进行合并操作处理
        _quicklistZiplistMerge(quicklist, next, next_next);
        next = next_next = NULL; /* they could have moved, invalidate them. */
    }

    /* Try to merge center node and previous node */
	//尝试进行前置节点向中间节点合并
    if (_quicklistNodeAllowMerge(center, center->prev, fill)) {
		//进行合并操作处理,这个地方获取了合并后的新的指向,主要是方便后期的合并操作处理------>即center指向节点可能已经发生了改变
        target = _quicklistZiplistMerge(quicklist, center->prev, center);
        center = NULL; /* center could have been deleted, invalidate it. */
    } else {
        /* else, we didn't merge here, but target needs to be valid below. */
        target = center;
    }

    /* Use result of center merge (or original) to merge with next node. */
	//尝试进行后置节点向中间节点合并
    if (_quicklistNodeAllowMerge(target, target->next, fill)) {
		//进行合并操作处理
        _quicklistZiplistMerge(quicklist, target, target->next);
    }
}

/* 进行拆分对应的结构节点为两个结构节点
 *   注意这个地方作者使用的技巧 分配相同大小的空间 对两个空间进行不同范围的删除元素的操作处理
 * Split 'node' into two parts, parameterized by 'offset' and 'after'.
 *
 * The 'after' argument controls which quicklistNode gets returned.
 * If 'after'==1, returned node has elements after 'offset'.
 *                input node keeps elements up to 'offset', including 'offset'.
 * If 'after'==0, returned node has elements up to 'offset', including 'offset'.
 *                input node keeps elements after 'offset'.
 *
 * If 'after'==1, returned node will have elements _after_ 'offset'.
 *                The returned node will have elements [OFFSET+1, END].
 *                The input node keeps elements [0, OFFSET].
 *
 * If 'after'==0, returned node will keep elements up to and including 'offset'.
 *                The returned node will have elements [0, OFFSET].
 *                The input node keeps elements [OFFSET+1, END].
 *
 * The input node keeps all elements not taken by the returned node.
 *
 * Returns newly created node or NULL if split not possible. 
 */
REDIS_STATIC quicklistNode *_quicklistSplitNode(quicklistNode *node, int offset, int after) {
    size_t zl_sz = node->sz;
	//创建对应的新结构节点
    quicklistNode *new_node = quicklistCreateNode();
	//分配足够大小的数据节点存储的空间
    new_node->zl = zmalloc(zl_sz);

    /* Copy original ziplist so we can split it */
	//拷贝原始结构节点的一份数据
    memcpy(new_node->zl, node->zl, zl_sz);

    /* -1 here means "continue deleting until the list ends" */
	//计算出两个结构节点进行删除对应范围的元素的偏移和个数
    int orig_start = after ? offset + 1 : 0;
    int orig_extent = after ? -1 : offset;
    int new_start = after ? 0 : offset;
    int new_extent = after ? offset + 1 : -1;

    D("After %d (%d); ranges: [%d, %d], [%d, %d]", after, offset, orig_start, orig_extent, new_start, new_extent);

    //最原始结构节点中删除对应范围的值
    node->zl = ziplistDeleteRange(node->zl, orig_start, orig_extent);
    node->count = ziplistLen(node->zl);
    quicklistNodeUpdateSz(node);

    //在新的结构节点中删除对应范围的值
    new_node->zl = ziplistDeleteRange(new_node->zl, new_start, new_extent);
    new_node->count = ziplistLen(new_node->zl);
    quicklistNodeUpdateSz(new_node);

    D("After split lengths: orig (%d), new (%d)", node->count, new_node->count);
	//返回新创建的结构节点
    return new_node;
}

/* 在给定的节点信息前或者后插入一个新的节点数据
 * Insert a new entry before or after existing entry 'entry'.
 *
 * If after==1, the new value is inserted after 'entry', otherwise the new value is inserted before 'entry'. 
 */
REDIS_STATIC void _quicklistInsert(quicklist *quicklist, quicklistEntry *entry, void *value, const size_t sz, int after) {
    int full = 0, at_tail = 0, at_head = 0, full_next = 0, full_prev = 0;
    int fill = quicklist->fill;
	//获取对应的需要插入元素节点信息
    quicklistNode *node = entry->node;
    quicklistNode *new_node = NULL;

    //检测给定的需要插入元素的节点是否存在
    if (!node) {
        /* we have no reference node, so let's create only node in the list */
        D("No node given!");
		//创建新的结构节点
        new_node = quicklistCreateNode();
	    //将对应的数据插入新创建的压缩列表中,并将其设置到结构节点上
        new_node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);
		//将新创建的结构节点添加到quicklist列表上
        __quicklistInsertNode(quicklist, NULL, new_node, after);
		//更新相关的quicklist列表的数据信息
        new_node->count++;
        quicklist->count++;
        return;
    }

    /* Populate accounting flags for easier boolean checks later */
	//检测给定的节点上是否允许进行插入数据
    if (!_quicklistNodeAllowInsert(node, fill, sz)) {
        D("Current node is full with count %d with requested fill %lu", node->count, fill);
		//设置本结构节点上的数据已经满的标识
        full = 1;
    }

    //检测是否是向后插入且插入的偏移等于节点元素的数量------->此处需要检测后置节点是否有空间进行插入本元素的处理
    if (after && (entry->offset == node->count)) {
        D("At Tail of current ziplist");
		//设置在后置节点中进行插入操作处理
        at_tail = 1;
	    //检测对应的后置节点是否允许插入操作处理
        if (!_quicklistNodeAllowInsert(node->next, fill, sz)) {
            D("Next node is full too.");
			//设置后置节点中已经满数据的标识
            full_next = 1;
        }
    }

	//检测是否是向前插入且插入的偏移等于开始位置------->此处需要检测前置节点是否有空间进行插入本元素的处理
    if (!after && (entry->offset == 0)) {
        D("At Head");
		//设置在前置节点中进行插入操作处理
        at_head = 1;
	    //检测对应的前置节点是否允许插入操作处理
        if (!_quicklistNodeAllowInsert(node->prev, fill, sz)) {
            D("Prev node is full too.");
			//设置前置节点中已经满数据的标识
            full_prev = 1;
        }
    }
    /* 通过上面的判断情况的判断,并进行数据插入操作处理 */
    /* Now determine where and how to insert the new element */
    if (!full && after) {
		//向本节点中后面插入,且本节点中有足够空间的处理情况
        D("Not full, inserting after current position.");
		//尝试进行压缩操作处理
        quicklistDecompressNodeForUse(node);
	    //获取对应的插入位置
        unsigned char *next = ziplistNext(node->zl, entry->zi);
		//根据获取到的插入位置进行数据插入操作处理
        if (next == NULL) {
            node->zl = ziplistPush(node->zl, value, sz, ZIPLIST_TAIL);
        } else {
            node->zl = ziplistInsert(node->zl, next, value, sz);
        }
		//更新结构节点的元素数量
        node->count++;
		//更新结构节点中记录的总字节数量
        quicklistNodeUpdateSz(node);
		//尝试进行压缩操作处理
        quicklistRecompressOnly(quicklist, node);
    } else if (!full && !after) {
		//向本节点中前面插入,且本节点中有足够空间的处理情况
        D("Not full, inserting before current position.");
        quicklistDecompressNodeForUse(node);
        node->zl = ziplistInsert(node->zl, entry->zi, value, sz);
        node->count++;
        quicklistNodeUpdateSz(node);
        quicklistRecompressOnly(quicklist, node);
    } else if (full && at_tail && node->next && !full_next && after) {
        /* If we are: at tail, next has free space, and inserting after:
         *   - insert entry at head of next node. */
        //本节点已经没有对应的空间 向后置节点进行插入 且后置节点有足够空间----->插入到后置节点的头部
        D("Full and tail, but next isn't full; inserting next node head");
        new_node = node->next;
        quicklistDecompressNodeForUse(new_node);
        new_node->zl = ziplistPush(new_node->zl, value, sz, ZIPLIST_HEAD);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        quicklistRecompressOnly(quicklist, new_node);
    } else if (full && at_head && node->prev && !full_prev && !after) {
        /* If we are: at head, previous has free space, and inserting before: insert entry at tail of previous node. */
        //本节点已经没有对应的空间 向前置节点进行插入 且前置节点有足够空间----->插入到前置节点的尾部
        D("Full and head, but prev isn't full, inserting prev node tail");
        new_node = node->prev;
        quicklistDecompressNodeForUse(new_node);
        new_node->zl = ziplistPush(new_node->zl, value, sz, ZIPLIST_TAIL);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        quicklistRecompressOnly(quicklist, new_node);
    } else if (full && ((at_tail && node->next && full_next && after) || (at_head && node->prev && full_prev && !after))) {
		//本节点已经没有对应的空间 且前后节点都没有足够的空间了----->需要重新分配结构节点进行插入操作处理
        /* If we are: full, and our prev/next is full, then: create new node and attach to quicklist */
        D("\tprovisioning new node...");
        new_node = quicklistCreateNode();
        new_node->zl = ziplistPush(ziplistNew(), value, sz, ZIPLIST_HEAD);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
        __quicklistInsertNode(quicklist, node, new_node, after);
    } else if (full) {
		//本节点已经没有对应的空间 且没有对应的前置和后置节点的处理情况----->需要进行拆分操作处理
        /* else, node is full we need to split it. */ covers both after and !after cases */
        D("\tsplitting node...");
        quicklistDecompressNodeForUse(node);
		//进行拆分操作处理
        new_node = _quicklistSplitNode(node, entry->offset, after);
        new_node->zl = ziplistPush(new_node->zl, value, sz, after ? ZIPLIST_HEAD : ZIPLIST_TAIL);
        new_node->count++;
        quicklistNodeUpdateSz(new_node);
		//插入拆分后新创建的结构节点
        __quicklistInsertNode(quicklist, node, new_node, after);
		//进一步进行合并操作处理
        _quicklistMergeNodes(quicklist, node);
    }
	//更新quicklist列表中元素的数量
    quicklist->count++;
}

/* 封装的在给定的节点信息前插入元素 */
void quicklistInsertBefore(quicklist *quicklist, quicklistEntry *entry, void *value, const size_t sz) {
    _quicklistInsert(quicklist, entry, value, sz, 0);
}

/* 封装的在给定的节点信息后插入元素 */
void quicklistInsertAfter(quicklist *quicklist, quicklistEntry *entry, void *value, const size_t sz) {
    _quicklistInsert(quicklist, entry, value, sz, 1);
}

/* 在quicklist中删除从指定索引位置开始的指定元素的数量
 * Delete a range of elements from the quicklist.
 *
 * elements may span across multiple quicklistNodes, so we have to be careful about tracking where we start and end.
 *
 * Returns 1 if entries were deleted, 0 if nothing was deleted. 
 */
int quicklistDelRange(quicklist *quicklist, const long start, const long count) {
    //检测给定的删除数量是否合法
    if (count <= 0)
        return 0;

    unsigned long extent = count; /* range is inclusive of start position */
    //根据给定的起始偏移和总的数量量来更新需要删除元素的数量值
    if (start >= 0 && extent > (quicklist->count - start)) {
        /* if requesting delete more elements than exist, limit to list size. */
        extent = quicklist->count - start;
    } else if (start < 0 && extent > (unsigned long)(-start)) {
        /* else, if at negative offset, limit max size to rest of list. */
	    //这个地方需要明确一个事情,如果是负索引,那么就是从负索引开始想后删除,而不是进行向前进行删除
        extent = -start; /* c.f. LREM -29 29; just delete until end. */
    }

    quicklistEntry entry;
	//获取给定的索引位置处的节点信息 并记录到对应的quicklistEntry结构中
    if (!quicklistIndex(quicklist, start, &entry))
        return 0;

    D("Quicklist delete request for start %ld, count %ld, extent: %ld", start, count, extent);
	//获取对应索引位置所处理的结构节点
    quicklistNode *node = entry.node;

    /* iterate over next nodes until everything is deleted. */
	//循环处理需要删除的元素的数量
    while (extent) {
		//记录下一次需要操作处理的结构节点
        quicklistNode *next = node->next;

        unsigned long del;
        int delete_entire_node = 0;
        if (entry.offset == 0 && extent >= node->count) {
            /* If we are deleting more than the count of this node, we can just delete the entire node without ziplist math. */
		    //此种情况需要进行删除对应的结构节点的处理
            delete_entire_node = 1;
			//记录本次删除的元素的个数
            del = node->count;
        } else if (entry.offset >= 0 && extent >= node->count) {
            /* If deleting more nodes after this one, calculate delete based on size of current node. */
		    //此种情况属于从偏移位置开始删除后面的所有元素
		    //记录本次删除的元素的个数
            del = node->count - entry.offset;
        } else if (entry.offset < 0) {
            /* If offset is negative, we are in the first run of this loop
             * and we are deleting the entire range
             * from this start offset to end of list.  Since the Negative
             * offset is the number of elements until the tail of the list,
             * just use it directly as the deletion count. 
             */
            //初始需要在本节点中删除的元素个数------->即索引位置向后的元素进行全部删除操作处理
            del = -entry.offset;

            /* If the positive offset is greater than the remaining extent, we only delete the remaining extent, not the entire offset.*/
			//检测个数是否够删除
            if (del > extent)
				//修复需要删除的数量
                del = extent;
        } else {
            /* else, we are deleting less than the extent of this node, so use extent directly. */
		    //此处仅是部分删除操作的情况-------->即删除部分元素就够了需要删除元素的数目
            del = extent;
        }

        D("[%ld]: asking to del: %ld because offset: %d; (ENTIRE NODE: %d), " "node count: %u", extent, del, entry.offset, delete_entire_node, node->count);
		//检测是否整体删除对应的结构节点
        if (delete_entire_node) {
			//删除对应的结构节点
            __quicklistDelNode(quicklist, node);
        } else {
            //尝试进行解压缩结构节点中的数据
            quicklistDecompressNodeForUse(node);
			//删除从指定索引位置开始的对应数目的元素
            node->zl = ziplistDeleteRange(node->zl, entry.offset, del);
			//更新对应的结构节点的总字节数量
            quicklistNodeUpdateSz(node);
			//更新结构节点中元素的个数
            node->count -= del;
			//更新quicklist中总元素的数量
            quicklist->count -= del;
			//检测有必要是否删除对应的结构节点
            quicklistDeleteIfEmpty(quicklist, node);
			//检测对应的结构节点是否存在
            if (node)
				//进行尝试压缩操作处理
                quicklistRecompressOnly(quicklist, node);
        }
		//减少需要删除的元素个数
        extent -= del;
		//设置下一次需要遍历的结构节点
        node = next;
		//设置对应的偏移位置
        entry.offset = 0;
    }
	//返回进行删除指定数量元素的成功标识
    return 1;
}

/* 比较给定的两个字符串数据指向的内容是否相同
 * Passthrough to ziplistCompare() 
 */
int quicklistCompare(unsigned char *p1, unsigned char *p2, int p2_len) {
    return ziplistCompare(p1, p2, p2_len);
}

/* 获取给定的quicklist列表的指定方向上的迭代器
 * Returns a quicklist iterator 'iter'. After the initialization every call to quicklistNext() will return the next element of the quicklist. 
 */
quicklistIter *quicklistGetIterator(const quicklist *quicklist, int direction) {
    quicklistIter *iter;
	//分配对应的迭代器占据的空间
    iter = zmalloc(sizeof(*iter));
	//根据设置的方向初始化迭代器中的相关数据
    if (direction == AL_START_HEAD) {
		//设置当前遍历到的结构节点时头结构节点
        iter->current = quicklist->head;
		//设置对应的偏移值
        iter->offset = 0;
    } else if (direction == AL_START_TAIL) {
		//设置当前遍历到的结构节点时尾结构节点
        iter->current = quicklist->tail;
		//设置对应的偏移值
        iter->offset = -1;
    }
	//设置方向
    iter->direction = direction;
	//设置quicklist列表的指向
    iter->quicklist = quicklist;
	//
    iter->zi = NULL;
	//返回对应的迭代器指向
    return iter;
}

/* 根据给定的索引位置和方向初始一个迭代器对象
 * Initialize an iterator at a specific offset 'idx' and make the iterator return nodes in 'direction' direction. 
 */
quicklistIter *quicklistGetIteratorAtIdx(const quicklist *quicklist, const int direction, const long long idx) {
    quicklistEntry entry;
	//首先根据提供的索引获取对应的结构节点信息------>即是否有对应索引的结构节点存在
    if (quicklistIndex(quicklist, idx, &entry)) {
		//创建对应的迭代器对象
        quicklistIter *base = quicklistGetIterator(quicklist, direction);
        base->zi = NULL;
	    //设置需要遍历的结构节点
        base->current = entry.node;
		//设置对应的偏移位置
        base->offset = entry.offset;
        return base;
    } else {
        return NULL;
    }
}

/* 释放对应的迭代器对象
 * Release iterator.
 * If we still have a valid current node, then re-encode current node. 
 */
void quicklistReleaseIterator(quicklistIter *iter) {
    if (iter->current)
		//尝试对迭代器遍历的当前节点进行压缩操作处理
        quicklistCompress(iter->quicklist, iter->current);
	//释放对应的迭代器占据的空间
    zfree(iter);
}

/* 根据给定的迭代器获取下一个可以遍历的元素数据
 * Get next element in iterator.
 *
 * Note: You must NOT insert into the list while iterating over it.
 * You *may* delete from the list while iterating using the
 * quicklistDelEntry() function.
 * If you insert into the quicklist while iterating, you should
 * re-create the iterator after your addition.
 *
 * iter = quicklistGetIterator(quicklist,<direction>);
 * quicklistEntry entry;
 * while (quicklistNext(iter, &entry)) {
 *     if (entry.value)
 *          [[ use entry.value with entry.sz ]]
 *     else
 *          [[ use entry.longval ]]
 * }
 *
 * Populates 'entry' with values for this iteration.
 * Returns 0 when iteration is complete or if iteration not possible.
 * If return value is 0, the contents of 'entry' are not valid.
 */
int quicklistNext(quicklistIter *iter, quicklistEntry *entry) {
    //初始化存储遍历元素的数据结构的数据信息
    initEntry(entry);
	
	//检测给定的迭代器是否为空
    if (!iter) {
        D("Returning because no iter!");
		//返回没有对应元素的标记处理
        return 0;
    }

    //设置对应的quicklist列表指向
    entry->quicklist = iter->quicklist;
	//设置当前需要遍历的结构节点指向
    entry->node = iter->current;

    //检测设置的需要遍历的结构节点是否为空
    if (!iter->current) {
        D("Returning because current node is NULL")
		//返回没有对应元素的标记处理
        return 0;
    }

    unsigned char *(*nextFn)(unsigned char *, unsigned char *) = NULL;
    int offset_update = 0;

    if (!iter->zi) {
        /* If !zi, use current index. */
        quicklistDecompressNodeForUse(iter->current);
        iter->zi = ziplistIndex(iter->current->zl, iter->offset);
    } else {
        /* else, use existing iterator offset and get prev/next as necessary. */
        if (iter->direction == AL_START_HEAD) {
            nextFn = ziplistNext;
            offset_update = 1;
        } else if (iter->direction == AL_START_TAIL) {
            nextFn = ziplistPrev;
            offset_update = -1;
        }
        iter->zi = nextFn(iter->current->zl, iter->zi);
        iter->offset += offset_update;
    }

    entry->zi = iter->zi;
    entry->offset = iter->offset;

    if (iter->zi) {
        /* Populate value from existing ziplist position */
        ziplistGet(entry->zi, &entry->value, &entry->sz, &entry->longval);
        return 1;
    } else {
        /* We ran out of ziplist entries. Pick next node, update offset, then re-run retrieval. */
        quicklistCompress(iter->quicklist, iter->current);
        if (iter->direction == AL_START_HEAD) {
            /* Forward traversal */
            D("Jumping to start of next node");
            iter->current = iter->current->next;
            iter->offset = 0;
        } else if (iter->direction == AL_START_TAIL) {
            /* Reverse traversal */
            D("Jumping to end of previous node");
            iter->current = iter->current->prev;
            iter->offset = -1;
        }
        iter->zi = NULL;
		//
        return quicklistNext(iter, entry);
    }
}

/* 拷贝对应的quicklist列表结构
 * Duplicate the quicklist.
 * On success a copy of the original quicklist is returned.
 *
 * The original quicklist both on success or error is never modified.
 *
 * Returns newly allocated quicklist. 
 */
quicklist *quicklistDup(quicklist *orig) {
    quicklist *copy;
	//创建新的quicklist列表结构
    copy = quicklistNew(orig->fill, orig->compress);

    //循环遍历quicklist列表中结构节点
    for (quicklistNode *current = orig->head; current; current = current->next) {
		//创建对应的结构节点
        quicklistNode *node = quicklistCreateNode();
		//检测当前结构节点的数据是否处于压缩状态
        if (current->encoding == QUICKLIST_NODE_ENCODING_LZF) {
			//获取对应的压缩数据指向
            quicklistLZF *lzf = (quicklistLZF *)current->zl;
			//计算需要开辟的空间个数
            size_t lzf_sz = sizeof(*lzf) + lzf->sz;
		    //进行分配空间操作处理
            node->zl = zmalloc(lzf_sz);
			//进行数据的拷贝操作处理
            memcpy(node->zl, current->zl, lzf_sz);
        } else if (current->encoding == QUICKLIST_NODE_ENCODING_RAW) {
			//分配对应的空间
            node->zl = zmalloc(current->sz);
			//进行数据的拷贝操作处理
            memcpy(node->zl, current->zl, current->sz);
        }
		//设置当前结构节点的元素总个数
        node->count = current->count;
		//设置quicklist列表中元素的总个数
        copy->count += node->count;
		//设置元素节点的指向
        node->sz = current->sz;
		//设置对应的是否进行压缩标识
        node->encoding = current->encoding;
		//将新创建的结构节点添加到新创建的quicklist列表结构中
        _quicklistInsertNodeAfter(copy, copy->tail, node);
    }

    /* copy->count must equal orig->count here */
	//返回新创建的quicklist列表指向
    return copy;
}

/* 获取指定索引位置处理的元素节点信息
 * Populate 'entry' with the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range 0 is returned.
 *
 * Returns 1 if element found
 * Returns 0 if element not found 
 */
int quicklistIndex(const quicklist *quicklist, const long long idx, quicklistEntry *entry) {
    quicklistNode *n;
    unsigned long long accum = 0;
    unsigned long long index;
	
    int forward = idx < 0 ? 0 : 1; /* < 0 -> reverse, 0+ -> forward */
	//初始化对应的quicklistEntry结构中的数据
    initEntry(entry);
	//设置当前查找的quicklist列表对象
    entry->quicklist = quicklist;

	//处理给定索引的正负问题
    if (!forward) {
        index = (-idx) - 1;
        n = quicklist->tail;
    } else {
        index = idx;
        n = quicklist->head;
    }

    //首先检测给定的需要查找的索引是否超过了quicklist列表中总的元素数量
    if (index >= quicklist->count)
        return 0;
	//循环操作处理 找到一个可以查找对应索引的结构节点的位置
    while (likely(n)) {
		//检测增加当前结构中元素个数是否超过了对应的索引值
        if ((accum + n->count) > index) {
			//找到了对应的结构节点
            break;
        } else {
            D("Skipping over (%p) %u at accum %lld", (void *)n, n->count, accum);
			//增加遍历的总元素个数
            accum += n->count;
			//设置下一个需要遍历的元素节点位置
            n = forward ? n->next : n->prev;
        }
    }
	//检测找到的结构节点是否为空
    if (!n)
		//返回没有找到对应索引位置上的元素标记出来
        return 0;

    D("Found node: %p at accum %llu, idx %llu, sub+ %llu, sub- %llu", (void *)n, accum, index, index - accum, (-index) - 1 + accum);
	//记录当前需要进一步查找的结构节点
    entry->node = n;
	//根据向前遍历还是向后查找来确定对应的节点偏移位置
    if (forward) {
        /* forward = normal head-to-tail offset. */
	    //计算需要找到的元素节点对应的偏移位置
        entry->offset = index - accum;
    } else {
        /* reverse = need negative offset for tail-to-head, so undo the result of the original if (index < 0) above. */
		//计算需要找到的元素节点对应的偏移位置
        entry->offset = (-index) - 1 + accum;
    }
	//尝试对给定的节点进行解压缩操作处理
    quicklistDecompressNodeForUse(entry->node);
	//获取对应索引位置上节点元素的信息------>即获取到的返回值 就是元素位置指向
    entry->zi = ziplistIndex(entry->node->zl, entry->offset);
	//获取对应位置上元素的信息,并将对应的信息存储到对应的位置上
    ziplistGet(entry->zi, &entry->value, &entry->sz, &entry->longval);
    /* The caller will use our result, so we don't re-compress here. The caller can recompress or delete the node as needed. */
	//返回找到对应索引上元素的信息标记出来
    return 1;
}

/* 
 * Rotate quicklist by moving the tail element to the head. 
 */
void quicklistRotate(quicklist *quicklist) {
    //检测对应的quicklist列表中的元素数量是否小于1 即不需要进行翻转操作处理
    if (quicklist->count <= 1)
        return;

    /* First, get the tail entry */
	//首先获取尾部结构节点上数据在最后一个元素
    unsigned char *p = ziplistIndex(quicklist->tail->zl, -1);
    unsigned char *value;
    long long longval;
    unsigned int sz;
    char longstr[32] = {0};
	//获取对应元素位置上的数据
    ziplistGet(p, &value, &sz, &longval);

    /* If value found is NULL, then ziplistGet populated longval instead */
	//检测是否是整数类型的数据
    if (!value) {
        /* Write the longval as a string so we can re-add it */
	    //获取整数对应的字符串类型数据
        sz = ll2string(longstr, sizeof(longstr), longval);
        value = (unsigned char *)longstr;
    }

    /* Add tail entry to head (must happen before tail is deleted). */
	//将对应的元素添加到头部结构节点的开始位置---->注意这个地方有可能造成ziplist整体位置的变化 所以记录的p有可能不准确了
    quicklistPushHead(quicklist, value, sz);

    /* If quicklist has only one node, the head ziplist is also the tail ziplist and PushHead() could have reallocated our single ziplist, which would make our pre-existing 'p' unusable. */
	//此处就是检查是否有一个结构节点来进一步明确需要删除的最后一个元素的位置
    if (quicklist->len == 1) {
		//获取需要删除的最后一个节点的位置指向
        p = ziplistIndex(quicklist->tail->zl, -1);
    }

    /* Remove tail entry. */
	//删除对应的移动的元素节点
    quicklistDelIndex(quicklist, quicklist->tail, &p);
}

/* 在quicklist列表中进行数据弹出操作处理的处理函数
 * pop from quicklist and return result in 'data' ptr.  Value of 'data'
 * is the return value of 'saver' function pointer if the data is NOT a number.
 *
 * If the quicklist element is a long long, then the return value is returned in
 * 'sval'.
 *
 * Return value of 0 means no elements available.
 * Return value of 1 means check 'data' and 'sval' for values.
 * If 'data' is set, use 'data' and 'sz'.  Otherwise, use 'sval'.
 */
int quicklistPopCustom(quicklist *quicklist, int where, unsigned char **data, unsigned int *sz, long long *sval, void *(*saver)(unsigned char *data, unsigned int sz)) {
    unsigned char *p;
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
	//首先确定进行弹出操作处理的位置
    int pos = (where == QUICKLIST_HEAD) ? 0 : -1;

    //检测quicklist列表中是否有对应的元素数据
    if (quicklist->count == 0)
        return 0;

    //给给定的存储空间进行相关初始化操作处理----->方便后期进行存储数据和判断是否设置了数据
    if (data)
        *data = NULL;
    if (sz)
        *sz = 0;
    if (sval)
        *sval = -123456789;

    quicklistNode *node;
	//根据给定的方向来确定需要操作的结构节点
    if (where == QUICKLIST_HEAD && quicklist->head) {
        node = quicklist->head;
    } else if (where == QUICKLIST_TAIL && quicklist->tail) {
        node = quicklist->tail;
    } else {
        return 0;
    }

    //获取对应节点上对应位置上的数据元素位置指向
    p = ziplistIndex(node->zl, pos);
	//获取对应位置上元素节点的数据信息
    if (ziplistGet(p, &vstr, &vlen, &vlong)) {
		//检测是否是字符串数据类型
        if (vstr) {
            if (data)
				//进行将对应的字符串数据设置到对应的位置指向--------->这个地方的这个函数具体不知道是如何进一步处理获取的字符串数据的转换操作处理的
				//_quicklistSaver  ------------->对给定的数据指向进行数据拷贝操作处理
                *data = saver(vstr, vlen);
            if (sz)
				//设置字符串数据的长度
                *sz = vlen;
        } else {
            if (data)
				//进一步置空字符串数据指向
                *data = NULL;
            if (sval)
				//设置对应的整数数据
                *sval = vlong;
        }
		//进行删除对应位置指向的元素节点的操作处理
        quicklistDelIndex(quicklist, node, &p);
		//返回找到对应的数据节点的标识
        return 1;
    }
	//返回没有找到对应的数据节点的标识
    return 0;
}

/* 对给定的数据指向进行数据拷贝操作处理
 * Return a malloc'd copy of data passed in 
 */
REDIS_STATIC void *_quicklistSaver(unsigned char *data, unsigned int sz) {
    unsigned char *vstr;
	//检测是否需要进行数据拷贝操作处理
    if (data) {
		//分配对应大小的空间
        vstr = zmalloc(sz);
		//进行数据元素拷贝操作处理
        memcpy(vstr, data, sz);
	    //返回拷贝完数据的对象空间的指向
        return vstr;
    }
    return NULL;
}

/* 默认的在quicklist列表中进行数据弹出操作的处理函数
 * Default pop function
 *
 * Returns malloc'd value from quicklist 
 */
int quicklistPop(quicklist *quicklist, int where, unsigned char **data, unsigned int *sz, long long *slong) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
	//首先检测quicklist列表中是否有数据可以进行弹出操作处理
    if (quicklist->count == 0)
		//返回弹出数据失败的标识
        return 0;
	//尝试进行弹出元素数据节点操作处理  同时将对应的数据填充中给定的指着指向的位置上
    int ret = quicklistPopCustom(quicklist, where, &vstr, &vlen, &vlong, _quicklistSaver);
    if (data)
		//设置对应的存储的字符串数据
        *data = vstr;
    if (slong)
		//设置对应的存储的整数数据
        *slong = vlong;
    if (sz)
		//设置对应的长度
        *sz = vlen;
	//返回进行弹出操作是否成功的标识
    return ret;
}

/* 封装的基于给定参数进行节点数据插入操作的处理---->注意这个地方是插入数据节点
 * Wrapper to allow argument-based switching between HEAD/TAIL pop 
 */
void quicklistPush(quicklist *quicklist, void *value, const size_t sz, int where) {
    if (where == QUICKLIST_HEAD) {
		//在头部进行插入数据节点
        quicklistPushHead(quicklist, value, sz);
    } else if (where == QUICKLIST_TAIL) {
        //在尾部进行插入数据节点
        quicklistPushTail(quicklist, value, sz);
    }
}

/* The rest of this file is test cases and test helpers. */
#ifdef REDIS_TEST
#include <stdint.h>
#include <sys/time.h>

#define assert(_e)                                                             \
    do {                                                                       \
        if (!(_e)) {                                                           \
            printf("\n\n=== ASSERTION FAILED ===\n");                          \
            printf("==> %s:%d '%s' is not true\n", __FILE__, __LINE__, #_e);   \
            err++;                                                             \
        }                                                                      \
    } while (0)

#define yell(str, ...) printf("ERROR! " str "\n\n", __VA_ARGS__)

#define OK printf("\tOK\n")

#define ERROR                                                                  \
    do {                                                                       \
        printf("\tERROR!\n");                                                  \
        err++;                                                                 \
    } while (0)

#define ERR(x, ...)                                                            \
    do {                                                                       \
        printf("%s:%s:%d:\t", __FILE__, __FUNCTION__, __LINE__);               \
        printf("ERROR! " x "\n", __VA_ARGS__);                                 \
        err++;                                                                 \
    } while (0)

#define TEST(name) printf("test — %s\n", name);
#define TEST_DESC(name, ...) printf("test — " name "\n", __VA_ARGS__);

#define QL_TEST_VERBOSE 0

#define UNUSED(x) (void)(x)
static void ql_info(quicklist *ql) {
#if QL_TEST_VERBOSE
    printf("Container length: %lu\n", ql->len);
    printf("Container size: %lu\n", ql->count);
    if (ql->head)
        printf("\t(zsize head: %d)\n", ziplistLen(ql->head->zl));
    if (ql->tail)
        printf("\t(zsize tail: %d)\n", ziplistLen(ql->tail->zl));
    printf("\n");
#else
    UNUSED(ql);
#endif
}

/* Return the UNIX time in microseconds */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec) * 1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
static long long mstime(void) { return ustime() / 1000; }

/* Iterate over an entire quicklist.
 * Print the list if 'print' == 1.
 *
 * Returns physical count of elements found by iterating over the list. */
static int _itrprintr(quicklist *ql, int print, int forward) {
    quicklistIter *iter =
        quicklistGetIterator(ql, forward ? AL_START_HEAD : AL_START_TAIL);
    quicklistEntry entry;
    int i = 0;
    int p = 0;
    quicklistNode *prev = NULL;
    while (quicklistNext(iter, &entry)) {
        if (entry.node != prev) {
            /* Count the number of list nodes too */
            p++;
            prev = entry.node;
        }
        if (print) {
            printf("[%3d (%2d)]: [%.*s] (%lld)\n", i, p, entry.sz,
                   (char *)entry.value, entry.longval);
        }
        i++;
    }
    quicklistReleaseIterator(iter);
    return i;
}
static int itrprintr(quicklist *ql, int print) {
    return _itrprintr(ql, print, 1);
}

static int itrprintr_rev(quicklist *ql, int print) {
    return _itrprintr(ql, print, 0);
}

#define ql_verify(a, b, c, d, e)                                               \
    do {                                                                       \
        err += _ql_verify((a), (b), (c), (d), (e));                            \
    } while (0)

/* Verify list metadata matches physical list contents. */
static int _ql_verify(quicklist *ql, uint32_t len, uint32_t count,
                      uint32_t head_count, uint32_t tail_count) {
    int errors = 0;

    ql_info(ql);
    if (len != ql->len) {
        yell("quicklist length wrong: expected %d, got %u", len, ql->len);
        errors++;
    }

    if (count != ql->count) {
        yell("quicklist count wrong: expected %d, got %lu", count, ql->count);
        errors++;
    }

    int loopr = itrprintr(ql, 0);
    if (loopr != (int)ql->count) {
        yell("quicklist cached count not match actual count: expected %lu, got "
             "%d",
             ql->count, loopr);
        errors++;
    }

    int rloopr = itrprintr_rev(ql, 0);
    if (loopr != rloopr) {
        yell("quicklist has different forward count than reverse count!  "
             "Forward count is %d, reverse count is %d.",
             loopr, rloopr);
        errors++;
    }

    if (ql->len == 0 && !errors) {
        OK;
        return errors;
    }

    if (ql->head && head_count != ql->head->count &&
        head_count != ziplistLen(ql->head->zl)) {
        yell("quicklist head count wrong: expected %d, "
             "got cached %d vs. actual %d",
             head_count, ql->head->count, ziplistLen(ql->head->zl));
        errors++;
    }

    if (ql->tail && tail_count != ql->tail->count &&
        tail_count != ziplistLen(ql->tail->zl)) {
        yell("quicklist tail count wrong: expected %d, "
             "got cached %u vs. actual %d",
             tail_count, ql->tail->count, ziplistLen(ql->tail->zl));
        errors++;
    }

    if (quicklistAllowsCompression(ql)) {
        quicklistNode *node = ql->head;
        unsigned int low_raw = ql->compress;
        unsigned int high_raw = ql->len - ql->compress;

        for (unsigned int at = 0; at < ql->len; at++, node = node->next) {
            if (node && (at < low_raw || at >= high_raw)) {
                if (node->encoding != QUICKLIST_NODE_ENCODING_RAW) {
                    yell("Incorrect compression: node %d is "
                         "compressed at depth %d ((%u, %u); total "
                         "nodes: %u; size: %u; recompress: %d)",
                         at, ql->compress, low_raw, high_raw, ql->len, node->sz,
                         node->recompress);
                    errors++;
                }
            } else {
                if (node->encoding != QUICKLIST_NODE_ENCODING_LZF &&
                    !node->attempted_compress) {
                    yell("Incorrect non-compression: node %d is NOT "
                         "compressed at depth %d ((%u, %u); total "
                         "nodes: %u; size: %u; recompress: %d; attempted: %d)",
                         at, ql->compress, low_raw, high_raw, ql->len, node->sz,
                         node->recompress, node->attempted_compress);
                    errors++;
                }
            }
        }
    }

    if (!errors)
        OK;
    return errors;
}

/* Generate new string concatenating integer i against string 'prefix' */
static char *genstr(char *prefix, int i) {
    static char result[64] = {0};
    snprintf(result, sizeof(result), "%s%d", prefix, i);
    return result;
}

/* main test, but callable from other files */
int quicklistTest(int argc, char *argv[]) {
    UNUSED(argc);
    UNUSED(argv);

    unsigned int err = 0;
    int optimize_start =
        -(int)(sizeof(optimization_level) / sizeof(*optimization_level));

    printf("Starting optimization offset at: %d\n", optimize_start);

    int options[] = {0, 1, 2, 3, 4, 5, 6, 10};
    size_t option_count = sizeof(options) / sizeof(*options);
    long long runtime[option_count];

    for (int _i = 0; _i < (int)option_count; _i++) {
        printf("Testing Option %d\n", options[_i]);
        long long start = mstime();

        TEST("create list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("add to tail of empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "hello", 6);
            /* 1 for head and 1 for tail beacuse 1 node = head = tail */
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("add to head of empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            /* 1 for head and 1 for tail beacuse 1 node = head = tail */
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        for (int f = optimize_start; f < 32; f++) {
            TEST_DESC("add to tail 5x at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 5; i++)
                    quicklistPushTail(ql, genstr("hello", i), 32);
                if (ql->count != 5)
                    ERROR;
                if (f == 32)
                    ql_verify(ql, 1, 5, 5, 5);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 32; f++) {
            TEST_DESC("add to head 5x at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 5; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                if (ql->count != 5)
                    ERROR;
                if (f == 32)
                    ql_verify(ql, 1, 5, 5, 5);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 512; f++) {
            TEST_DESC("add to tail 500x at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i), 64);
                if (ql->count != 500)
                    ERROR;
                if (f == 32)
                    ql_verify(ql, 16, 500, 32, 20);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 512; f++) {
            TEST_DESC("add to head 500x at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                if (ql->count != 500)
                    ERROR;
                if (f == 32)
                    ql_verify(ql, 16, 500, 20, 32);
                quicklistRelease(ql);
            }
        }

        TEST("rotate empty") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistRotate(ql);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        for (int f = optimize_start; f < 32; f++) {
            TEST("rotate one val once") {
                quicklist *ql = quicklistNew(f, options[_i]);
                quicklistPushHead(ql, "hello", 6);
                quicklistRotate(ql);
                /* Ignore compression verify because ziplist is
                 * too small to compress. */
                ql_verify(ql, 1, 1, 1, 1);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 3; f++) {
            TEST_DESC("rotate 500 val 5000 times at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                quicklistPushHead(ql, "900", 3);
                quicklistPushHead(ql, "7000", 4);
                quicklistPushHead(ql, "-1200", 5);
                quicklistPushHead(ql, "42", 2);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 64);
                ql_info(ql);
                for (int i = 0; i < 5000; i++) {
                    ql_info(ql);
                    quicklistRotate(ql);
                }
                if (f == 1)
                    ql_verify(ql, 504, 504, 1, 1);
                else if (f == 2)
                    ql_verify(ql, 252, 504, 2, 2);
                else if (f == 32)
                    ql_verify(ql, 16, 504, 32, 24);
                quicklistRelease(ql);
            }
        }

        TEST("pop empty") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPop(ql, QUICKLIST_HEAD, NULL, NULL, NULL);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop 1 string from 1") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            char *populate = genstr("hello", 331);
            quicklistPushHead(ql, populate, 32);
            unsigned char *data;
            unsigned int sz;
            long long lv;
            ql_info(ql);
            quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
            assert(data != NULL);
            assert(sz == 32);
            if (strcmp(populate, (char *)data))
                ERR("Pop'd value (%.*s) didn't equal original value (%s)", sz,
                    data, populate);
            zfree(data);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 1 number from 1") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "55513", 5);
            unsigned char *data;
            unsigned int sz;
            long long lv;
            ql_info(ql);
            quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
            assert(data == NULL);
            assert(lv == 55513);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 500 from 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_info(ql);
            for (int i = 0; i < 500; i++) {
                unsigned char *data;
                unsigned int sz;
                long long lv;
                int ret = quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                assert(ret == 1);
                assert(data != NULL);
                assert(sz == 32);
                if (strcmp(genstr("hello", 499 - i), (char *)data))
                    ERR("Pop'd value (%.*s) didn't equal original value (%s)",
                        sz, data, genstr("hello", 499 - i));
                zfree(data);
            }
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("pop head 5000 from 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            for (int i = 0; i < 5000; i++) {
                unsigned char *data;
                unsigned int sz;
                long long lv;
                int ret = quicklistPop(ql, QUICKLIST_HEAD, &data, &sz, &lv);
                if (i < 500) {
                    assert(ret == 1);
                    assert(data != NULL);
                    assert(sz == 32);
                    if (strcmp(genstr("hello", 499 - i), (char *)data))
                        ERR("Pop'd value (%.*s) didn't equal original value "
                            "(%s)",
                            sz, data, genstr("hello", 499 - i));
                    zfree(data);
                } else {
                    assert(ret == 0);
                }
            }
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("iterate forward over 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
            quicklistEntry entry;
            int i = 499, count = 0;
            while (quicklistNext(iter, &entry)) {
                char *h = genstr("hello", i);
                if (strcmp((char *)entry.value, h))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, h, i);
                i--;
                count++;
            }
            if (count != 500)
                ERR("Didn't iterate over exactly 500 elements (%d)", i);
            ql_verify(ql, 16, 500, 20, 32);
            quicklistReleaseIterator(iter);
            quicklistRelease(ql);
        }

        TEST("iterate reverse over 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
            quicklistEntry entry;
            int i = 0;
            while (quicklistNext(iter, &entry)) {
                char *h = genstr("hello", i);
                if (strcmp((char *)entry.value, h))
                    ERR("value [%s] didn't match [%s] at position %d",
                        entry.value, h, i);
                i++;
            }
            if (i != 500)
                ERR("Didn't iterate over exactly 500 elements (%d)", i);
            ql_verify(ql, 16, 500, 20, 32);
            quicklistReleaseIterator(iter);
            quicklistRelease(ql);
        }

        TEST("insert before with 0 elements") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertBefore(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("insert after with 0 elements") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 1, 1, 1);
            quicklistRelease(ql);
        }

        TEST("insert after 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 2, 2, 2);
            quicklistRelease(ql);
        }

        TEST("insert before 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, "hello", 6);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            quicklistInsertAfter(ql, &entry, "abc", 4);
            ql_verify(ql, 1, 2, 2, 2);
            quicklistRelease(ql);
        }

        for (int f = optimize_start; f < 12; f++) {
            TEST_DESC("insert once in elements while iterating at fill %d at "
                      "compress %d\n",
                      f, options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                quicklistPushTail(ql, "abc", 3);
                quicklistSetFill(ql, 1);
                quicklistPushTail(ql, "def", 3); /* force to unique node */
                quicklistSetFill(ql, f);
                quicklistPushTail(ql, "bob", 3); /* force to reset for +3 */
                quicklistPushTail(ql, "foo", 3);
                quicklistPushTail(ql, "zoo", 3);

                itrprintr(ql, 0);
                /* insert "bar" before "bob" while iterating over list. */
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
                quicklistEntry entry;
                while (quicklistNext(iter, &entry)) {
                    if (!strncmp((char *)entry.value, "bob", 3)) {
                        /* Insert as fill = 1 so it spills into new node. */
                        quicklistInsertBefore(ql, &entry, "bar", 3);
                        break; /* didn't we fix insert-while-iterating? */
                    }
                }
                itrprintr(ql, 0);

                /* verify results */
                quicklistIndex(ql, 0, &entry);
                if (strncmp((char *)entry.value, "abc", 3))
                    ERR("Value 0 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 1, &entry);
                if (strncmp((char *)entry.value, "def", 3))
                    ERR("Value 1 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 2, &entry);
                if (strncmp((char *)entry.value, "bar", 3))
                    ERR("Value 2 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 3, &entry);
                if (strncmp((char *)entry.value, "bob", 3))
                    ERR("Value 3 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 4, &entry);
                if (strncmp((char *)entry.value, "foo", 3))
                    ERR("Value 4 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistIndex(ql, 5, &entry);
                if (strncmp((char *)entry.value, "zoo", 3))
                    ERR("Value 5 didn't match, instead got: %.*s", entry.sz,
                        entry.value);
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 1024; f++) {
            TEST_DESC(
                "insert [before] 250 new in middle of 500 elements at fill"
                " %d at compress %d",
                f, options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i), 32);
                for (int i = 0; i < 250; i++) {
                    quicklistEntry entry;
                    quicklistIndex(ql, 250, &entry);
                    quicklistInsertBefore(ql, &entry, genstr("abc", i), 32);
                }
                if (f == 32)
                    ql_verify(ql, 25, 750, 32, 20);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 1024; f++) {
            TEST_DESC("insert [after] 250 new in middle of 500 elements at "
                      "fill %d at compress %d",
                      f, options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushHead(ql, genstr("hello", i), 32);
                for (int i = 0; i < 250; i++) {
                    quicklistEntry entry;
                    quicklistIndex(ql, 250, &entry);
                    quicklistInsertAfter(ql, &entry, genstr("abc", i), 32);
                }

                if (ql->count != 750)
                    ERR("List size not 750, but rather %ld", ql->count);

                if (f == 32)
                    ql_verify(ql, 26, 750, 20, 32);
                quicklistRelease(ql);
            }
        }

        TEST("duplicate empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            ql_verify(ql, 0, 0, 0, 0);
            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 0, 0, 0, 0);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        TEST("duplicate list of 1 element") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushHead(ql, genstr("hello", 3), 32);
            ql_verify(ql, 1, 1, 1, 1);
            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 1, 1, 1, 1);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        TEST("duplicate list of 500") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 16, 500, 20, 32);

            quicklist *copy = quicklistDup(ql);
            ql_verify(copy, 16, 500, 20, 32);
            quicklistRelease(ql);
            quicklistRelease(copy);
        }

        for (int f = optimize_start; f < 512; f++) {
            TEST_DESC("index 1,200 from 500 list at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, 1, &entry);
                if (!strcmp((char *)entry.value, "hello2"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistIndex(ql, 200, &entry);
                if (!strcmp((char *)entry.value, "hello201"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index -1,-2 from 500 list at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, -1, &entry);
                if (!strcmp((char *)entry.value, "hello500"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistIndex(ql, -2, &entry);
                if (!strcmp((char *)entry.value, "hello499"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index -100 from 500 list at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 500; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                quicklistIndex(ql, -100, &entry);
                if (!strcmp((char *)entry.value, "hello401"))
                    OK;
                else
                    ERR("Value: %s", entry.value);
                quicklistRelease(ql);
            }

            TEST_DESC("index too big +1 from 50 list at fill %d at compress %d",
                      f, options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                for (int i = 0; i < 50; i++)
                    quicklistPushTail(ql, genstr("hello", i + 1), 32);
                quicklistEntry entry;
                if (quicklistIndex(ql, 50, &entry))
                    ERR("Index found at 50 with 50 list: %.*s", entry.sz,
                        entry.value);
                else
                    OK;
                quicklistRelease(ql);
            }
        }

        TEST("delete range empty list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistDelRange(ql, 5, 20);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete range of entire node in list of one node") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 32; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 1, 32, 32, 32);
            quicklistDelRange(ql, 0, 32);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete range of entire node with overflow counts") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            for (int i = 0; i < 32; i++)
                quicklistPushHead(ql, genstr("hello", i), 32);
            ql_verify(ql, 1, 32, 32, 32);
            quicklistDelRange(ql, 0, 128);
            ql_verify(ql, 0, 0, 0, 0);
            quicklistRelease(ql);
        }

        TEST("delete middle 100 of 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, 200, 100);
            ql_verify(ql, 14, 400, 32, 20);
            quicklistRelease(ql);
        }

        TEST("delete negative 1 from 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, -1, 1);
            ql_verify(ql, 16, 499, 32, 19);
            quicklistRelease(ql);
        }

        TEST("delete negative 1 from 500 list with overflow counts") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 16, 500, 32, 20);
            quicklistDelRange(ql, -1, 128);
            ql_verify(ql, 16, 499, 32, 19);
            quicklistRelease(ql);
        }

        TEST("delete negative 100 from 500 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 500; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            quicklistDelRange(ql, -100, 100);
            ql_verify(ql, 13, 400, 32, 16);
            quicklistRelease(ql);
        }

        TEST("delete -10 count 5 from 50 list") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            for (int i = 0; i < 50; i++)
                quicklistPushTail(ql, genstr("hello", i + 1), 32);
            ql_verify(ql, 2, 50, 32, 18);
            quicklistDelRange(ql, -10, 5);
            ql_verify(ql, 2, 45, 32, 13);
            quicklistRelease(ql);
        }

        TEST("numbers only list read") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "1111", 4);
            quicklistPushTail(ql, "2222", 4);
            quicklistPushTail(ql, "3333", 4);
            quicklistPushTail(ql, "4444", 4);
            ql_verify(ql, 1, 4, 4, 4);
            quicklistEntry entry;
            quicklistIndex(ql, 0, &entry);
            if (entry.longval != 1111)
                ERR("Not 1111, %lld", entry.longval);
            quicklistIndex(ql, 1, &entry);
            if (entry.longval != 2222)
                ERR("Not 2222, %lld", entry.longval);
            quicklistIndex(ql, 2, &entry);
            if (entry.longval != 3333)
                ERR("Not 3333, %lld", entry.longval);
            quicklistIndex(ql, 3, &entry);
            if (entry.longval != 4444)
                ERR("Not 4444, %lld", entry.longval);
            if (quicklistIndex(ql, 4, &entry))
                ERR("Index past elements: %lld", entry.longval);
            quicklistIndex(ql, -1, &entry);
            if (entry.longval != 4444)
                ERR("Not 4444 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -2, &entry);
            if (entry.longval != 3333)
                ERR("Not 3333 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -3, &entry);
            if (entry.longval != 2222)
                ERR("Not 2222 (reverse), %lld", entry.longval);
            quicklistIndex(ql, -4, &entry);
            if (entry.longval != 1111)
                ERR("Not 1111 (reverse), %lld", entry.longval);
            if (quicklistIndex(ql, -5, &entry))
                ERR("Index past elements (reverse), %lld", entry.longval);
            quicklistRelease(ql);
        }

        TEST("numbers larger list read") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistSetFill(ql, 32);
            char num[32];
            long long nums[5000];
            for (int i = 0; i < 5000; i++) {
                nums[i] = -5157318210846258176 + i;
                int sz = ll2string(num, sizeof(num), nums[i]);
                quicklistPushTail(ql, num, sz);
            }
            quicklistPushTail(ql, "xxxxxxxxxxxxxxxxxxxx", 20);
            quicklistEntry entry;
            for (int i = 0; i < 5000; i++) {
                quicklistIndex(ql, i, &entry);
                if (entry.longval != nums[i])
                    ERR("[%d] Not longval %lld but rather %lld", i, nums[i],
                        entry.longval);
                entry.longval = 0xdeadbeef;
            }
            quicklistIndex(ql, 5000, &entry);
            if (strncmp((char *)entry.value, "xxxxxxxxxxxxxxxxxxxx", 20))
                ERR("String val not match: %s", entry.value);
            ql_verify(ql, 157, 5001, 32, 9);
            quicklistRelease(ql);
        }

        TEST("numbers larger list read B") {
            quicklist *ql = quicklistNew(-2, options[_i]);
            quicklistPushTail(ql, "99", 2);
            quicklistPushTail(ql, "98", 2);
            quicklistPushTail(ql, "xxxxxxxxxxxxxxxxxxxx", 20);
            quicklistPushTail(ql, "96", 2);
            quicklistPushTail(ql, "95", 2);
            quicklistReplaceAtIndex(ql, 1, "foo", 3);
            quicklistReplaceAtIndex(ql, -1, "bar", 3);
            quicklistRelease(ql);
            OK;
        }

        for (int f = optimize_start; f < 16; f++) {
            TEST_DESC("lrem test at fill %d at compress %d", f, options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                char *words[] = {"abc", "foo", "bar",  "foobar", "foobared",
                                 "zap", "bar", "test", "foo"};
                char *result[] = {"abc", "foo",  "foobar", "foobared",
                                  "zap", "test", "foo"};
                char *resultB[] = {"abc",      "foo", "foobar",
                                   "foobared", "zap", "test"};
                for (int i = 0; i < 9; i++)
                    quicklistPushTail(ql, words[i], strlen(words[i]));

                /* lrem 0 bar */
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_HEAD);
                quicklistEntry entry;
                int i = 0;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(entry.zi, (unsigned char *)"bar", 3)) {
                        quicklistDelEntry(iter, &entry);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                /* check result of lrem 0 bar */
                iter = quicklistGetIterator(ql, AL_START_HEAD);
                i = 0;
                int ok = 1;
                while (quicklistNext(iter, &entry)) {
                    /* Result must be: abc, foo, foobar, foobared, zap, test,
                     * foo */
                    if (strncmp((char *)entry.value, result[i], entry.sz)) {
                        ERR("No match at position %d, got %.*s instead of %s",
                            i, entry.sz, entry.value, result[i]);
                        ok = 0;
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                quicklistPushTail(ql, "foo", 3);

                /* lrem -2 foo */
                iter = quicklistGetIterator(ql, AL_START_TAIL);
                i = 0;
                int del = 2;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(entry.zi, (unsigned char *)"foo", 3)) {
                        quicklistDelEntry(iter, &entry);
                        del--;
                    }
                    if (!del)
                        break;
                    i++;
                }
                quicklistReleaseIterator(iter);

                /* check result of lrem -2 foo */
                /* (we're ignoring the '2' part and still deleting all foo
                 * because
                 * we only have two foo) */
                iter = quicklistGetIterator(ql, AL_START_TAIL);
                i = 0;
                size_t resB = sizeof(resultB) / sizeof(*resultB);
                while (quicklistNext(iter, &entry)) {
                    /* Result must be: abc, foo, foobar, foobared, zap, test,
                     * foo */
                    if (strncmp((char *)entry.value, resultB[resB - 1 - i],
                                entry.sz)) {
                        ERR("No match at position %d, got %.*s instead of %s",
                            i, entry.sz, entry.value, resultB[resB - 1 - i]);
                        ok = 0;
                    }
                    i++;
                }

                quicklistReleaseIterator(iter);
                /* final result of all tests */
                if (ok)
                    OK;
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 16; f++) {
            TEST_DESC("iterate reverse + delete at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                quicklistPushTail(ql, "abc", 3);
                quicklistPushTail(ql, "def", 3);
                quicklistPushTail(ql, "hij", 3);
                quicklistPushTail(ql, "jkl", 3);
                quicklistPushTail(ql, "oop", 3);

                quicklistEntry entry;
                quicklistIter *iter = quicklistGetIterator(ql, AL_START_TAIL);
                int i = 0;
                while (quicklistNext(iter, &entry)) {
                    if (quicklistCompare(entry.zi, (unsigned char *)"hij", 3)) {
                        quicklistDelEntry(iter, &entry);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);

                if (i != 5)
                    ERR("Didn't iterate 5 times, iterated %d times.", i);

                /* Check results after deletion of "hij" */
                iter = quicklistGetIterator(ql, AL_START_HEAD);
                i = 0;
                char *vals[] = {"abc", "def", "jkl", "oop"};
                while (quicklistNext(iter, &entry)) {
                    if (!quicklistCompare(entry.zi, (unsigned char *)vals[i],
                                          3)) {
                        ERR("Value at %d didn't match %s\n", i, vals[i]);
                    }
                    i++;
                }
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 800; f++) {
            TEST_DESC("iterator at index test at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 760; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }

                quicklistEntry entry;
                quicklistIter *iter =
                    quicklistGetIteratorAtIdx(ql, AL_START_HEAD, 437);
                int i = 437;
                while (quicklistNext(iter, &entry)) {
                    if (entry.longval != nums[i])
                        ERR("Expected %lld, but got %lld", entry.longval,
                            nums[i]);
                    i++;
                }
                quicklistReleaseIterator(iter);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 40; f++) {
            TEST_DESC("ltrim test A at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 32; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (f == 32)
                    ql_verify(ql, 1, 32, 32, 32);
                /* ltrim 25 53 (keep [25,32] inclusive = 7 remaining) */
                quicklistDelRange(ql, 0, 25);
                quicklistDelRange(ql, 0, 0);
                quicklistEntry entry;
                for (int i = 0; i < 7; i++) {
                    quicklistIndex(ql, i, &entry);
                    if (entry.longval != nums[25 + i])
                        ERR("Deleted invalid range!  Expected %lld but got "
                            "%lld",
                            entry.longval, nums[25 + i]);
                }
                if (f == 32)
                    ql_verify(ql, 1, 7, 7, 7);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 40; f++) {
            TEST_DESC("ltrim test B at fill %d at compress %d", f,
                      options[_i]) {
                /* Force-disable compression because our 33 sequential
                 * integers don't compress and the check always fails. */
                quicklist *ql = quicklistNew(f, QUICKLIST_NOCOMPRESS);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (f == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                /* ltrim 5 16 (keep [5,16] inclusive = 12 remaining) */
                quicklistDelRange(ql, 0, 5);
                quicklistDelRange(ql, -16, 16);
                if (f == 32)
                    ql_verify(ql, 1, 12, 12, 12);
                quicklistEntry entry;
                quicklistIndex(ql, 0, &entry);
                if (entry.longval != 5)
                    ERR("A: longval not 5, but %lld", entry.longval);
                else
                    OK;
                quicklistIndex(ql, -1, &entry);
                if (entry.longval != 16)
                    ERR("B! got instead: %lld", entry.longval);
                else
                    OK;
                quicklistPushTail(ql, "bobobob", 7);
                quicklistIndex(ql, -1, &entry);
                if (strncmp((char *)entry.value, "bobobob", 7))
                    ERR("Tail doesn't match bobobob, it's %.*s instead",
                        entry.sz, entry.value);
                for (int i = 0; i < 12; i++) {
                    quicklistIndex(ql, i, &entry);
                    if (entry.longval != nums[5 + i])
                        ERR("Deleted invalid range!  Expected %lld but got "
                            "%lld",
                            entry.longval, nums[5 + i]);
                }
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 40; f++) {
            TEST_DESC("ltrim test C at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (f == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                /* ltrim 3 3 (keep [3,3] inclusive = 1 remaining) */
                quicklistDelRange(ql, 0, 3);
                quicklistDelRange(ql, -29,
                                  4000); /* make sure not loop forever */
                if (f == 32)
                    ql_verify(ql, 1, 1, 1, 1);
                quicklistEntry entry;
                quicklistIndex(ql, 0, &entry);
                if (entry.longval != -5157318210846258173)
                    ERROR;
                else
                    OK;
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 40; f++) {
            TEST_DESC("ltrim test D at fill %d at compress %d", f,
                      options[_i]) {
                quicklist *ql = quicklistNew(f, options[_i]);
                char num[32];
                long long nums[5000];
                for (int i = 0; i < 33; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    quicklistPushTail(ql, num, sz);
                }
                if (f == 32)
                    ql_verify(ql, 2, 33, 32, 1);
                quicklistDelRange(ql, -12, 3);
                if (ql->count != 30)
                    ERR("Didn't delete exactly three elements!  Count is: %lu",
                        ql->count);
                quicklistRelease(ql);
            }
        }

        for (int f = optimize_start; f < 72; f++) {
            TEST_DESC("create quicklist from ziplist at fill %d at compress %d",
                      f, options[_i]) {
                unsigned char *zl = ziplistNew();
                long long nums[64];
                char num[64];
                for (int i = 0; i < 33; i++) {
                    nums[i] = -5157318210846258176 + i;
                    int sz = ll2string(num, sizeof(num), nums[i]);
                    zl =
                        ziplistPush(zl, (unsigned char *)num, sz, ZIPLIST_TAIL);
                }
                for (int i = 0; i < 33; i++) {
                    zl = ziplistPush(zl, (unsigned char *)genstr("hello", i),
                                     32, ZIPLIST_TAIL);
                }
                quicklist *ql = quicklistCreateFromZiplist(f, options[_i], zl);
                if (f == 1)
                    ql_verify(ql, 66, 66, 1, 1);
                else if (f == 32)
                    ql_verify(ql, 3, 66, 32, 2);
                else if (f == 66)
                    ql_verify(ql, 1, 66, 66, 66);
                quicklistRelease(ql);
            }
        }

        long long stop = mstime();
        runtime[_i] = stop - start;
    }

    /* Run a longer test of compression depth outside of primary test loop. */
    int list_sizes[] = {250, 251, 500, 999, 1000};
    long long start = mstime();
    for (int list = 0; list < (int)(sizeof(list_sizes) / sizeof(*list_sizes));
         list++) {
        for (int f = optimize_start; f < 128; f++) {
            for (int depth = 1; depth < 40; depth++) {
                /* skip over many redundant test cases */
                TEST_DESC("verify specific compression of interior nodes with "
                          "%d list "
                          "at fill %d at compress %d",
                          list_sizes[list], f, depth) {
                    quicklist *ql = quicklistNew(f, depth);
                    for (int i = 0; i < list_sizes[list]; i++) {
                        quicklistPushTail(ql, genstr("hello TAIL", i + 1), 64);
                        quicklistPushHead(ql, genstr("hello HEAD", i + 1), 64);
                    }

                    quicklistNode *node = ql->head;
                    unsigned int low_raw = ql->compress;
                    unsigned int high_raw = ql->len - ql->compress;

                    for (unsigned int at = 0; at < ql->len;
                         at++, node = node->next) {
                        if (at < low_raw || at >= high_raw) {
                            if (node->encoding != QUICKLIST_NODE_ENCODING_RAW) {
                                ERR("Incorrect compression: node %d is "
                                    "compressed at depth %d ((%u, %u); total "
                                    "nodes: %u; size: %u)",
                                    at, depth, low_raw, high_raw, ql->len,
                                    node->sz);
                            }
                        } else {
                            if (node->encoding != QUICKLIST_NODE_ENCODING_LZF) {
                                ERR("Incorrect non-compression: node %d is NOT "
                                    "compressed at depth %d ((%u, %u); total "
                                    "nodes: %u; size: %u; attempted: %d)",
                                    at, depth, low_raw, high_raw, ql->len,
                                    node->sz, node->attempted_compress);
                            }
                        }
                    }
                    quicklistRelease(ql);
                }
            }
        }
    }
    long long stop = mstime();

    printf("\n");
    for (size_t i = 0; i < option_count; i++)
        printf("Test Loop %02d: %0.2f seconds.\n", options[i],
               (float)runtime[i] / 1000);
    printf("Compressions: %0.2f seconds.\n", (float)(stop - start) / 1000);
    printf("\n");

    if (!err)
        printf("ALL TESTS PASSED!\n");
    else
        ERR("Sorry, not all tests passed!  In fact, %d tests failed.", err);

    return err;
}
#endif













