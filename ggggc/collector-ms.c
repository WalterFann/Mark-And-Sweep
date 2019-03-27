#include "ggggc/gc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include "ggggc-internals.h"

#ifdef __cplusplus
extern "C" {
#endif


#ifdef GGGGC_DEBUG_MEMORY_CORRUPTION
static void memoryCorruptionCheckObj(const char *when, struct GGGGC_Header *obj)
{
    struct GGGGC_Descriptor *descriptor = obj->descriptor__ptr;
    void **objVp = (void **) (obj);
    ggc_size_t curWord, curDescription = 0, curDescriptorWord = 0;
    if (obj->ggggc_memoryCorruptionCheck != GGGGC_MEMORY_CORRUPTION_VAL) {
        fprintf(stderr, "GGGGC: Memory corruption (%s)!\n", when);
        abort();
    }
    if (descriptor->pointers[0] & 1) {
        /* it has pointers */
        for (curWord = 0; curWord < descriptor->size; curWord++) {
            if (curWord % GGGGC_BITS_PER_WORD == 0)
                curDescription = descriptor->pointers[curDescriptorWord++];
            if (curDescription & 1) {
                /* it's a pointer */
                struct GGGGC_Header *nobj = (struct GGGGC_Header *) objVp[curWord];
                if (nobj && nobj->ggggc_memoryCorruptionCheck != GGGGC_MEMORY_CORRUPTION_VAL) {
                    fprintf(stderr, "GGGGC: Memory corruption (%s nested)!\n", when);
                    abort();
                }
            }
            curDescription >>= 1;
        }
    } else {
        /* no pointers other than the descriptor */
        struct GGGGC_Header *nobj = (struct GGGGC_Header *) objVp[0];
        if (nobj && nobj->ggggc_memoryCorruptionCheck != GGGGC_MEMORY_CORRUPTION_VAL) {
            fprintf(stderr, "GGGGC: Memory corruption (%s nested)!\n", when);
            abort();
        }
    }
}

static void memoryCorruptionCheck(const char *when)
{
    struct GGGGC_PoolList *plCur;
    struct GGGGC_Pool *poolCur;
    struct GGGGC_PointerStackList *pslCur;
    struct GGGGC_PointerStack *psCur;
    unsigned char genCur;

    for (plCur = ggggc_rootPool0List; plCur; plCur = plCur->next) {
        for (poolCur = plCur->pool; poolCur; poolCur = poolCur->next) {
            struct GGGGC_Header *obj = (struct GGGGC_Header *) poolCur->start;
            for (; obj < (struct GGGGC_Header *) poolCur->free;
                 obj = (struct GGGGC_Header *) (((ggc_size_t) obj) + obj->descriptor__ptr->size * sizeof(ggc_size_t))) {
                memoryCorruptionCheckObj(when, obj);
            }
        }
    }

    for (genCur = 1; genCur < GGGGC_GENERATIONS; genCur++) {
        for (poolCur = ggggc_gens[genCur]; poolCur; poolCur = poolCur->next) {
            struct GGGGC_Header *obj = (struct GGGGC_Header *) poolCur->start;
            for (; obj < (struct GGGGC_Header *) poolCur->free;
                 obj = (struct GGGGC_Header *) (((ggc_size_t) obj) + obj->descriptor__ptr->size * sizeof(ggc_size_t))) {
                memoryCorruptionCheckObj(when, obj);
            }
        }
    }

    for (pslCur = ggggc_rootPointerStackList; pslCur; pslCur = pslCur->next) {
        for (psCur = pslCur->pointerStack; psCur; psCur = psCur->next) {
            ggc_size_t i;
            for (i = 0; i < psCur->size; i++) {
                struct GGGGC_Header *obj = *((struct GGGGC_Header **) psCur->pointers[i]);
                if (obj)
                    memoryCorruptionCheckObj(when, obj);
            }
        }
    }
}
#endif



/* allocate an object */
void *ggggc_malloc(struct GGGGC_Descriptor *descriptor)
{
    struct GGGGC_Header *ret = (struct GGGGC_Header *) ggggc_mallocRaw(&descriptor, descriptor->size);
    ret->descriptor__ptr = descriptor;
    return ret;
}

/* explicitly yield to the collector */
int ggggc_yield(){
	return 0;
}

#define max(a,b) (((a) > (b)) ? (a) : (b))


void* ggggc_FreeWithoutMask(void* ptr){
	struct GGGGC_FreeObject* freeObj = (struct GGGGC_FreeObject*)ptr;
	return (struct GGGGC_FreeObject*)((ggc_size_t) freeObj->next & (ggc_size_t) -3l);
}


void *ggggc_mallocRaw(struct GGGGC_Descriptor **descriptor, /* descriptor to protect, if applicable */
                      ggc_size_t size /* size of object to allocate */
                      ) 
{
    struct GGGGC_Pool *pool;
    struct GGGGC_Header *ret;
    int mustExpand = 0;

retry:
    /* get our allocation pool */
    if (ggggc_pool0) {
        pool = ggggc_pool0;
    } else {
        ggggc_gen0 = ggggc_pool0 = pool = ggggc_newPool(1);
    }

    /* do we have enough space? */
    int usingFree = 0;
    struct GGGGC_FreeObject *curFree = NULL;
    struct GGGGC_FreeObject *prevFree = NULL;
    //first check free list
    if(pool->freeList){
    	curFree = pool->freeList;
    	while(curFree){
    		//if enough space to split, do it
    		if(curFree->size >= max(sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t), size) + sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t))
    		{
    			//printf("using freelist and split\n");
    			size_t tempSize = curFree->size;
    			struct GGGGC_FreeObject * tempFree = curFree;
    			ret = (struct GGGGC_Header *) tempFree;
    			curFree = (struct GGGGC_FreeObject *)((ggc_size_t *)curFree + max(sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t), size));
    			curFree->size = tempSize-max(sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t), size);
    			curFree->next = tempFree->next; //already marked free
    			ret->descriptor__ptr = NULL;
#ifdef GGGGC_DEBUG_MEMORY_CORRUPTION
        		/* set its canary */
        		ret->ggggc_memoryCorruptionCheck = GGGGC_MEMORY_CORRUPTION_VAL;
#endif
        		memset(ret + 1, 0,  max(sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t), size) * sizeof(ggc_size_t) - sizeof(struct GGGGC_Header));
    			if(prevFree){
    				prevFree->next = curFree;
    				ggggc_markFree((void*)prevFree);
    			}
    			else{
    				pool->freeList = curFree;
    			}
    			usingFree = 1;
    			break;
    		}
    		else if(curFree->size == size){
    			//printf("using freelist and no split\n");
    			struct GGGGC_FreeObject* next =  ggggc_FreeWithoutMask((void*)curFree);
    			ret = (struct GGGGC_Header* )curFree;
    			ret->descriptor__ptr = NULL;
#ifdef GGGGC_DEBUG_MEMORY_CORRUPTION
        		/* set its canary */
        		ret->ggggc_memoryCorruptionCheck = GGGGC_MEMORY_CORRUPTION_VAL;
#endif
        		memset(ret+1, 0,  size * sizeof(ggc_size_t) - sizeof(struct GGGGC_Header));
        		if(prevFree){
        			prevFree->next = next;
        			ggggc_markFree((void*)prevFree);
    			}
    			else{
    				pool->freeList = next;
    			}
    			usingFree = 1;
    			break;
    		}
    		prevFree = curFree;
    		curFree = ggggc_FreeWithoutMask((void*)curFree);
    	}
    }

    if(!usingFree){
    	if(pool->end - pool->free >= max(sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t), size)){
    		/* good, allocate here */
        	ret = (struct GGGGC_Header *) pool->free;
        	pool->free += max(sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t), size);

        	ret->descriptor__ptr = NULL;
#ifdef GGGGC_DEBUG_MEMORY_CORRUPTION
        	/* set its canary */
        	ret->ggggc_memoryCorruptionCheck = GGGGC_MEMORY_CORRUPTION_VAL;
#endif

        	/* and clear the rest (necessary since this goes to the untrusted mutator) */
        	memset(ret + 1, 0, max(sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t), size) * sizeof(ggc_size_t) - sizeof(struct GGGGC_Header));

    	} 
    	else if (pool->next) {
        	ggggc_pool0 = pool = pool->next;
        	goto retry;
    	}
    	else {
    		/* need to collect, which means we need to actually be a GC-safe function */
        	GGC_PUSH_1(*descriptor);
        	ggggc_collect0(0);
        	GGC_POP();
        	//check whether to expand pool list
        	ggggc_myExpandPoolList(ggggc_gen0, ggggc_newPool, 1, mustExpand);
        	mustExpand = 1;
        	pool = ggggc_gen0;
        	goto retry;
    	}
    }

    return ret;
}


//Worklist DataStructure
struct WorklistNode
{
	void* data;
	struct WorklistNode* next;
};

//global worklist
struct WorklistNode* Worklist;

void WORKLIST_INIT(){
	Worklist = (struct WorklistNode* )malloc(sizeof(struct WorklistNode));
	Worklist->data = NULL;
	Worklist->next = NULL;
}

void WORKLIST_PUSH(void* ptr){
	struct WorklistNode* newNode = (struct WorklistNode* )malloc(sizeof(struct WorklistNode));
	newNode->data = ptr;
	newNode->next = Worklist;
	Worklist = newNode;
}

//maybe do manual GC, ie free

void* WORKLIST_POP(){
	if(Worklist->data){
		void* ret = Worklist->data;
		Worklist = Worklist->next;
		return ret;
	}
	else
		return NULL;
}

void ggggc_markObject(void* ptr){
	struct GGGGC_Header* header = (struct GGGGC_Header* )ptr;
	header->descriptor__ptr = (struct GGGGC_Descriptor *) ((ggc_size_t) header->descriptor__ptr | 1l );
}

void ggggc_unmarkObject(void* ptr){
	struct GGGGC_Header* header = (struct GGGGC_Header* )ptr;
	header->descriptor__ptr = (struct GGGGC_Descriptor *) ((ggc_size_t) header->descriptor__ptr & (ggc_size_t) -2l);
}

ggc_size_t ggggc_isMarked(void* ptr){
	return (ggc_size_t) ((struct GGGGC_Header *) ptr)->descriptor__ptr & 1l;
}

void ggggc_markFree(void* ptr){
	struct GGGGC_FreeObject* freeObj = (struct GGGGC_FreeObject* )ptr;
	freeObj->next = (struct GGGGC_FreeObject *) ((ggc_size_t) freeObj->next | 2l );
}

void ggggc_unmarkFree(void* ptr){
	struct GGGGC_FreeObject* freeObj = (struct GGGGC_FreeObject* )ptr;
	freeObj->next = (struct GGGGC_FreeObject *) ((ggc_size_t) freeObj->next & (ggc_size_t) -3l);
}

ggc_size_t ggggc_isFreeObject(void* ptr){
	return (ggc_size_t) ((struct GGGGC_FreeObject *) ptr)->next & 2l;
}

void ggggc_markWorklist(){
	void* ptr = WORKLIST_POP();
	struct GGGGC_Header* header;
	struct GGGGC_Descriptor* descriptor;
	struct GGGGC_Header* next;
	int counter = 1;
	while(ptr!=NULL){
		if(ggggc_isMarked(ptr)){
			ptr = WORKLIST_POP();
			continue;
		}
		header = (struct GGGGC_Header*)ptr;
		descriptor = header->descriptor__ptr;
		//mark first to get rid of infinite loop
		ggggc_markObject(ptr);
		//printf("mark %d obj\n", counter++);
		//printf("descriptor %p\n", descriptor);
		//reduce check time
		if(descriptor->pointers[0] & 1l){
			//printf("pointers %zx\n", descriptor->pointers[0]);
			//printf("size %d\n", descriptor->size);
			ggc_size_t bitMask;
			int bit = 0;
			while(bit<descriptor->size){
				bitMask = 1l << (bit%(sizeof(ggc_size_t)*8));
				if(descriptor->pointers[bit/(sizeof(ggc_size_t)*8)] & bitMask){
					//printf("%d\n", bit);
					struct GGGGC_Header** newHeader = (struct GGGGC_Header**)(((ggc_size_t *)ptr)+bit);
					//printf("newHeader: %p\n", newHeader);
					if(*newHeader){
						next = *newHeader;
						if(bit==0){
							//already marked, so clean it
							next = (struct GGGGC_Header*) descriptor;
						}
						WORKLIST_PUSH((void* )next);
						//printf("%p\n", next->descriptor__ptr);
					}
				}
				bit++;
			}
		}
		else{
			WORKLIST_PUSH((void*)descriptor);
		}
		ptr = WORKLIST_POP();
	}
}

void ggggc_mark(){
	struct GGGGC_PoolList pool0Node, *plCur;
    struct GGGGC_Pool *poolCur;
    struct GGGGC_PointerStackList pointerStackNode, *pslCur;
    struct GGGGC_PointerStack *psCur;
    struct GGGGC_JITPointerStackList jitPointerStackNode, *jpslCur;
    void **jpsCur;
    struct GGGGC_Header** header;
    ggc_size_t i;


    pool0Node.pool = ggggc_gen0;
    pool0Node.next = ggggc_blockedThreadPool0s;
    ggggc_rootPool0List = &pool0Node;
    pointerStackNode.pointerStack = ggggc_pointerStack;
    pointerStackNode.next = ggggc_blockedThreadPointerStacks;
    ggggc_rootPointerStackList = &pointerStackNode;
    jitPointerStackNode.cur = ggc_jitPointerStack;
    jitPointerStackNode.top = ggc_jitPointerStackTop;
    jitPointerStackNode.next = ggggc_blockedThreadJITPointerStacks;
    ggggc_rootJITPointerStackList = &jitPointerStackNode;

#ifdef GGGGC_DEBUG_MEMORY_CORRUPTION
    memoryCorruptionCheck("pre-collection");
#endif

    /* add our roots to the work list */
    for (pslCur = ggggc_rootPointerStackList; pslCur; pslCur = pslCur->next) {
        for (psCur = pslCur->pointerStack; psCur; psCur = psCur->next) {
            for (i = 0; i < psCur->size; i++) {
            	header = (struct GGGGC_Header **) (psCur->pointers[i]);
            	if(*header && !ggggc_isMarked((void* )*header)){
            		//printf("one ps\n");
            		WORKLIST_PUSH((void*)*header);
                	ggggc_markWorklist();
            	}
            }
        }
    }
    for (jpslCur = ggggc_rootJITPointerStackList; jpslCur; jpslCur = jpslCur->next) {
        for (jpsCur = jpslCur->cur; jpsCur < jpslCur->top; jpsCur++) {
            header = (struct GGGGC_Header **) jpsCur;
            if(*header && !ggggc_isMarked((void* )*header)){
            	//printf("one jitps\n");
            	WORKLIST_PUSH((void*)*header);
                ggggc_markWorklist();
            }
        }
    }
}

void ggggc_sweep(){
	struct GGGGC_Pool *pool = ggggc_gen0;
	int counter = 1;
	while(pool){
		//printf("sweep pool %d\n", counter++);
		ggc_size_t * curPos = (ggc_size_t*)pool->start;
		pool->freeList = NULL;
		struct GGGGC_FreeObject* newFree;
		struct GGGGC_Header* header;
		size_t size;
		pool->survivors = 0;
		while(curPos<pool->free){
			//printf("curPos: %p\n", curPos);
			if(ggggc_isMarked((void*)curPos)){
				ggggc_unmarkObject((void*)curPos);
				header = (struct GGGGC_Header*) curPos;
				size = header->descriptor__ptr->size;
				pool->survivors += size;
				//printf("%zx\n", header->descriptor__ptr);
				//printf("find %d obj\n", pool->survivors);
			}
			else{
				if(ggggc_isFreeObject((void*)curPos)){
					newFree = (struct GGGGC_FreeObject*) curPos;
					size = newFree->size;
					newFree->next = pool->freeList;
					pool->freeList = newFree;
					ggggc_markFree((void*)curPos);
				}
				else{
					//printf("find one unused obj\n");
					header = (struct GGGGC_Header*) curPos;
					//printf("%zx\n", header->descriptor__ptr);
					size = header->descriptor__ptr->size;
					newFree = (struct GGGGC_FreeObject*) curPos;
					newFree->size = size;
					newFree->next = pool->freeList;
					pool->freeList = newFree;
					ggggc_markFree((void*)curPos);
				}
			}
			//printf("size: %d\n", size);
			curPos = curPos + max(sizeof(struct GGGGC_FreeObject)/sizeof(ggc_size_t), size);
		}
		pool = pool->next;
	}
}


void ggggc_collect0(unsigned char gen){
	WORKLIST_INIT();
	//printf("mark phase\n");
	ggggc_mark();
	//printf("sweep phase\n");
	ggggc_sweep();
}

void ggggc_myExpandPoolList(struct GGGGC_Pool *poolList,
                          struct GGGGC_Pool *(*newPool)(struct GGGGC_Pool *),
                          int ratio, int mustExpand)
{
    struct GGGGC_Pool *pool = poolList;
    ggc_size_t space, survivors, poolCt;

    if (!pool) return;

    /* first figure out how much space was used */
    space = 0;
    survivors = 0;
    poolCt = 0;
    while (1) {
        space += pool->end - pool->start;
        survivors += pool->survivors;
        pool->survivors = 0;
        poolCt++;
        if (!pool->next) break;
        pool = pool->next;
    }

    /* now decide if it's too much */
    //printf("survivors %d\n", survivors);
    //printf("space: %d\n", space);
    if ((survivors<<ratio) > space|| mustExpand) {
        /* allocate more */
        //printf("expand pool\n");
        ggc_size_t i;
        for (i = 0; i < poolCt; i++) {
            pool->next = newPool(poolList);
            pool = pool->next;
            if (!pool) break;
        }
    }
}

#ifdef __cplusplus
}
#endif