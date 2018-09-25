/* 
 * adlist.c - A generic doubly linked list implementation
 *    一个通用的双向链表的实现方式
 */

#include <stdlib.h>
#include "adlist.h"
#include "zmalloc.h"

/* 创建一个新的双向链表结构
 * Create a new list. The created list can be freed with AlFreeList(), but private value of every node need to be freed by the user before to call AlFreeList().
 *
 * On error, NULL is returned. Otherwise the pointer to the new list. 
 */
list *listCreate(void) {
    //声明指向双向链表结构的指针
    struct list *list;
	//给双向链表结构分配对应的空间 并将对应的指针指向此空间结构
    if ((list = zmalloc(sizeof(*list))) == NULL)
        return NULL;
	//初始化双向链表空间中的数据
    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;
	//最终返回对应的双向列表结构的指针指向
    return list;
}

/* 清空给定的双向链表结构中的所有元素
 * Remove all the elements from the list without destroying the list itself. 
 */
void listEmpty(list *list) {
    unsigned long len;
    listNode *current, *next;
	//首先记录需要释放的头元素节点位置
    current = list->head;
	//记录需要删除的双向链表中元素的总个数
    len = list->len;
	//循环删除双向链表中所有的元素
    while(len--) {
		//必须记录对应的当前元素节点的下一个元素,方便后期向下进行循环
        next = current->next;
		//检查是否设定了进行是否元素的处理函数
        if (list->free) 
			//释放对应的元素值占据的空间--------->  元素节点中的值部分的释放
			list->free(current->value);
		//释放当前的元素节点占据的空间----------->元素节点整体结构的释放
        zfree(current);
		//设定需要释放的下一个节点
        current = next;
    }
	//重新初始化对应的双向链表结构的数据
    list->head = list->tail = NULL;
    list->len = 0;
}

/* 释放整个双向链表结构占据的空间(所有元素节点占据的空间 + 双向链表结构占据的空间)
 * Free the whole list.
 *
 * This function can't fail.
 */
void listRelease(list *list) {
    //首先释放双向链表结构中元素节点占据的空间
    listEmpty(list);
	//然后释放对应的双向链表结构占据的空间
    zfree(list);
}

/* 在双向链表的头部插入一个元素节点
 * Add a new node to the list, to head, containing the specified 'value' pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. 
 */
list *listAddNodeHead(list *list, void *value) {
    listNode *node;

    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
    node->value = value;
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    } else {
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }
    list->len++;
    return list;
}

/* 在双向链表的尾部插入一个元素节点
 * Add a new node to the list, to tail, containing the specified 'value' pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned.
 */
list *listAddNodeTail(list *list, void *value) {
    listNode *node;
	//首先给对应的节点分配对应的空间
    if ((node = zmalloc(sizeof(*node))) == NULL)
		//如果空间分配失败 就直接返回空指针   代表未对双向链表结构进行插入数据成功
        return NULL;
	//给元素节点设置元素值的指向
    node->value = value;
	//分析当前的双向链表结构中是否有元素 来确定如何进行插入处理
    if (list->len == 0) {
		//设置双向链表的头和尾全部指向本新创建的元素节点
        list->head = list->tail = node;
		//设置新元素节点的前和后指针指向
        node->prev = node->next = NULL;
    } else {
		//首先将新节点插入到尾部
        node->prev = list->tail;
        node->next = NULL;
		//然后重置双向链表结构的尾节点指针指向
        list->tail->next = node;
        list->tail = node;
    }
	//添加双向链表元素个数
    list->len++;
	//返回对应的双向链表结构指针指向
    return list;
}

/*
 * 在给定双向链表指定节点的前或者后进行插入元素处理
 * after 0 为前  1 为后
 */
list *listInsertNode(list *list, listNode *old_node, void *value, int after) {
    listNode *node;
	//首先进行元素节点空间分配处理
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;
	//设置对应的元素的值的指向
    node->value = value;
	//检查处理是在给定节点的前还是后进行插入处理
    if (after) {
		//将新节点插入到指定的节点的后面                   注意此处这是处理了新节点的插入问题并没有处理老节点的前后指向问题
        node->prev = old_node;
        node->next = old_node->next;
	    //检查给定的节点是否是尾节点                处理尾节点的指向问题
        if (list->tail == old_node) {
            list->tail = node;
        }
    } else {
        //将新节点插入到指定的节点的前面                   注意此处这是处理了新节点的插入问题并没有处理老节点的前后指向问题
        node->next = old_node;
        node->prev = old_node->prev;
		//检查给定的节点是否是头节点                处理头节点的指向问题
        if (list->head == old_node) {
            list->head = node;
        }
    }
	//处理前节点的后指向问题
    if (node->prev != NULL) {
        node->prev->next = node;
    }
	//处理后节点的前指向问题
    if (node->next != NULL) {
        node->next->prev = node;
    }
	//给双向列表添加元素个数
    list->len++;
	//返回对应的双向链表结构指向
    return list;
}

/* 在双向链表机构中删除指定的节点元素
 * Remove the specified node from the specified list.
 * It's up to the caller to free the private value of the node.
 *
 * This function can't fail. 
 */
void listDelNode(list *list, listNode *node) {
    //检测是否是头节点的处理策略
    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;
	//检测是否是尾节点的处理策略
    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;
	//检测是否配置释放元素节点值的释放空间处理函数
    if (list->free) 
		list->free(node->value);
	//释放对应的节点元素空间
    zfree(node);
	//减少对应的节点元素个数
    list->len--;
}

/* 根据给定的双向链表结构和对应的方向获取对应的双向链表的迭代器指针对象
 * Returns a list iterator 'iter'. After the initialization every
 * call to listNext() will return the next element of the list.
 *
 * This function can't fail. 
 */
listIter *listGetIterator(list *list, int direction) {
    listIter *iter;
	//首先分配对应的迭代器结构的空间
    if ((iter = zmalloc(sizeof(*iter))) == NULL) 
		return NULL;
	//根据给定的方向来确定下一个需要迭代的元素的指针指向
    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;
	//设置迭代器对象的方向
    iter->direction = direction;
	//返回对应的迭代器对象的指针指向
    return iter;
}

/* 释放对应的双向列表结构的迭代器对象的空间
 * Release the iterator memory 
 */
void listReleaseIterator(listIter *iter) {
    //释放对应的迭代器结构的空间
    zfree(iter);
}

/* 重置给定的迭代器为给定双向链表结构的前置迭代器
 * Create an iterator in the list private iterator structure
 */
void listRewind(list *list, listIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

/* 重置给定的迭代器为给定双向链表结构的后置迭代器
 * Create an iterator in the list private iterator structure
 */
void listRewindTail(list *list, listIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}

/* 给定给定的迭代器获取双向链表结构中的下一个元素节点指向
 * Return the next element of an iterator.
 * It's valid to remove the currently returned element using
 * listDelNode(), but not to remove other elements.
 *
 * The function returns a pointer to the next element of the list,
 * or NULL if there are no more elements, so the classical usage patter
 * is:
 *
 * iter = listGetIterator(list,<direction>);
 * while ((node = listNext(iter)) != NULL) {
 *     doSomethingWith(listNodeValue(node));
 * }
 *
 */
listNode *listNext(listIter *iter) {
    //获取迭代器指向的节点元素指向
    listNode *current = iter->next;
	//配置下一个需要遍历的节点元素的指向
    if (current != NULL) {
        if (iter->direction == AL_START_HEAD)
            iter->next = current->next;
        else
            iter->next = current->prev;
    }
	//返回对应的节点元素的指向
    return current;
}

/* 整体进行拷贝原始的双向链表结构,从而创建出一个一模一样的双向链表结构
 * Duplicate the whole list. On out of memory NULL is returned.
 * On success a copy of the original list is returned.
 *
 * The 'Dup' method set with listSetDupMethod() function is used
 * to copy the node value. Otherwise the same pointer value of
 * the original node is used as value of the copied node.
 *
 * The original list both on success or error is never modified. 
 */
list *listDup(list *orig) {
    list *copy;
	//注意这个地方是实体变量空间
    listIter iter;
    listNode *node;

    //首先进行检测创建对应的双向链表结构是否成功
    if ((copy = listCreate()) == NULL)
        return NULL;
	//初始化双向链表结构的处理函数
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;

	//获取原始双向链表结构的前置迭代器对象
    listRewind(orig, &iter);

	//循环变量原始双向链表中的元素
    while((node = listNext(&iter)) != NULL) {
        void *value;
		//检测是否配置了值拷贝处理函数
        if (copy->dup) {
			//获取值拷贝处理的指针指向
            value = copy->dup(node->value);
			//检测进行拷贝原始双向链表中的元素值是否成功
            if (value == NULL) {
				//拷贝值失败 需要释放对应的新的链表结构
                listRelease(copy);
				//返回空指向
                return NULL;
            }
        } else
            //没有对应的拷贝处理函数就直接拷贝值
            value = node->value;
		//拷贝值处理完成后,检测是否能够将对应的值元素插入到新的链表中
        if (listAddNodeTail(copy, value) == NULL) {
			//插入失败 需要释放对应的新的链表结构
            listRelease(copy);
			//返回空指向
            return NULL;
        }
    }
	//上述拷贝操作处理完成后 返回新的链表结构指向
    return copy;
}

/* 在双向链表结构中查询是否有指定值的节点元素
 * Search the list for a node matching a given key.
 * The match is performed using the 'match' method
 * set with listSetMatchMethod(). If no 'match' method
 * is set, the 'value' pointer of every node is directly
 * compared with the 'key' pointer.
 *
 * On success the first matching node pointer is returned
 * (search starts from head). If no matching node exists
 * NULL is returned. 
 */
listNode *listSearchKey(list *list, void *key) {
    listIter iter;
    listNode *node;
	//获取双向链表结构的前置迭代器
    listRewind(list, &iter);
	//循环遍历所有的元素节点
    while((node = listNext(&iter)) != NULL) {
		//检测是否配置了比较处理函数
        if (list->match) {
			//进行比较操作处理
            if (list->match(node->value, key)) {
				//值相同返回对应的节点
                return node;
            }
        } else {
            if (key == node->value) {
                return node;
            }
        }
    }
	//遍历完成后没有发现值相同的 直接返回空指向
    return NULL;
}

/* 获取双向链表结构中指定索引位置的元素节点
 * Return the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range NULL is returned. 
 */
listNode *listIndex(list *list, long index) {
    listNode *n;
	//首先根据给定索引来确定从前或者从后进行获取
    if (index < 0) {
		//计算后置索引位置
        index = (-index)-1;
		//设置开始遍历的节点为尾节点
        n = list->tail;
	    //循环进行获取指定索引位置上的元素处理
        while(index-- && n) 
			n = n->prev;
    } else {
        //设置开始遍历的节点为头节点
        n = list->head;
		//循环进行获取指定索引位置上的元素处理
        while(index-- && n) 
			n = n->next;
    }
	//返回找到的索引位置上的节点元素
    return n;
}

/* 将尾元素节点删除并插入到双向链表结构的头部
 * Rotate the list removing the tail node and inserting it to the head. 
 */
void listRotate(list *list) {
    //首先记录对应的尾元素节点指向
    listNode *tail = list->tail;
	//检测链表元素个数来确定是否进行移动元素处理
    if (listLength(list) <= 1) 
		return;

    /* Detach current tail */
	//首先设定新的尾元素节点
    list->tail = tail->prev;
    list->tail->next = NULL;
    /* Move it as head */
	//将尾节点元素移动到头部
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}

/* 将一个双向链表结构中的元素全部转移到一个给定的双向链表结构中
 * Add all the elements of the list 'o' at the end of the list 'l'. The list 'other' remains empty but otherwise valid. 
 */
void listJoin(list *l, list *o) {
    //首先检测是否有对应的头节点指向
    if (o->head)
		//将头拼接到指定节点的尾部               这里相当于将前置指向处理完成
        o->head->prev = l->tail;

    if (l->tail)
		//这里相当于将后置指向处理完成
        l->tail->next = o->head;
    else
        l->head = o->head;

    //检测是否需要重置尾节点的指向
    if (o->tail) 
		l->tail = o->tail;
	//重新计算元素个数
    l->len += o->len;

    /* Setup other as an empty list. */
	//重置双向链表结构的数据
    o->head = o->tail = NULL;
    o->len = 0;
}



