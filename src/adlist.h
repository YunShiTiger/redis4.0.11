/* 
 * adlist.h - A generic doubly linked list implementation
 *    一个通用的双向链表的实现方式
 */

#ifndef __ADLIST_H__
#define __ADLIST_H__

/* 双向链表结构中的          节点结构      迭代器结构       整体数据结构
 * Node, List, and Iterator are the only data structures used currently. 
 */
typedef struct listNode {
    //节点前置指向
    struct listNode *prev;
	//节点后置指向
    struct listNode *next;
	//节点值指向
    void *value;
} listNode;

typedef struct listIter {
	//指向的下一个节点元素
    listNode *next;
	//迭代器的方向
    int direction;
} listIter;

typedef struct list {
	//头结点指向
    listNode *head;
	//尾节点指向
    listNode *tail;
	//值拷贝处理函数
    void *(*dup)(void *ptr);
	//值释放处理函数
    void (*free)(void *ptr);
	//值比较处理函数
    int (*match)(void *ptr, void *key);
	//双向链表中元素个数
    unsigned long len;
} list;

/* Functions implemented as macros */
/* 对外提供的宏处理函数 */
#define listLength(l) ((l)->len)
#define listFirst(l) ((l)->head)
#define listLast(l) ((l)->tail)
#define listPrevNode(n) ((n)->prev)
#define listNextNode(n) ((n)->next)
#define listNodeValue(n) ((n)->value)

#define listSetDupMethod(l,m) ((l)->dup = (m))
#define listSetFreeMethod(l,m) ((l)->free = (m))
#define listSetMatchMethod(l,m) ((l)->match = (m))

#define listGetDupMethod(l) ((l)->dup)
#define listGetFree(l) ((l)->free)
#define listGetMatchMethod(l) ((l)->match)

/* Prototypes */
/* 对外提供操作双向链表结构的处理函数 */
list *listCreate(void);
void listRelease(list *list);
void listEmpty(list *list);
list *listAddNodeHead(list *list, void *value);
list *listAddNodeTail(list *list, void *value);
list *listInsertNode(list *list, listNode *old_node, void *value, int after);
void listDelNode(list *list, listNode *node);
listIter *listGetIterator(list *list, int direction);
listNode *listNext(listIter *iter);
void listReleaseIterator(listIter *iter);
list *listDup(list *orig);
listNode *listSearchKey(list *list, void *key);
listNode *listIndex(list *list, long index);
void listRewind(list *list, listIter *li);
void listRewindTail(list *list, listIter *li);
void listRotate(list *list);
void listJoin(list *l, list *o);

/* Directions for iterators */
/* 迭代器方向的宏定义 */
#define AL_START_HEAD 0
#define AL_START_TAIL 1

#endif /* __ADLIST_H__ */






