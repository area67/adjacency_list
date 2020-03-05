#ifndef MDLIST_H
#define MDLIST_H

#include <cstdint>
#include <string>
#include <cmath>
#include "pre_alloc.h"
#include "lftt.h"

#define SET_ADPINV(_p)    ((MDNode *)(((uintptr_t)(_p)) | 1))
#define CLR_ADPINV(_p)    ((MDNode *)(((uintptr_t)(_p)) & ~1))
#define IS_ADPINV(_p)     (((uintptr_t)(_p)) & 1)

#define SET_DELINV(_p)    ((MDNode *)(((uintptr_t)(_p)) | 2))
#define CLR_DELINV(_p)    ((MDNode *)(((uintptr_t)(_p)) & ~2))
#define IS_DELINV(_p)     (((uintptr_t)(_p)) & 2)

#define CLR_INVALID(_p)    ((MDNode *)(((uintptr_t)(_p)) & ~3))
#define IS_INVALID(_p)     (((uintptr_t)(_p)) & 3)

static const uint32_t DIMENSION = 16;

struct MDDesc;

struct MDNode
{
    MDNode* m_child[DIMENSION];
    uint8_t m_coord[DIMENSION];
    uint32_t m_key;             //key
    MDDesc* m_pending;            //pending operation to adopt children 

    NodeDesc* node_desc;
};

//Any insertion as a child of node in the rage [pred_dim, dim] needs to help finish the task
struct MDDesc
{
    MDNode* curr;
    uint8_t pred_dim;              //dimension of pred node
    uint8_t dim;                   //dimension of this node
};

class MDList 
{

public:
    MDList (uint32_t key_range, MDNode *head, PreAllocator<MDDesc> *& d_allocator);
    ~MDList ();
    
    bool Insert(MDNode*& new_node, MDNode*& pred, MDNode*& curr, uint32_t& dim, uint32_t& pred_dim);
    bool Delete(MDNode*& pred, MDNode*& curr, uint32_t pred_dim, uint32_t dim);
    bool Find(uint32_t key);

public:
    //Procedures used by Insert()
    template<int D>
    void KeyToCoord(uint32_t key, uint8_t coord[]);

    void LocatePred(uint8_t coord[], MDNode*& pred, MDNode*& curr, uint32_t& dim, uint32_t& pred_dim);
    MDDesc* FillNewNode(MDNode* new_node, MDNode*& pred, MDNode*& curr, uint32_t& dim, uint32_t& pred_dim);
    void FinishInserting(MDNode* n, MDDesc* desc);

    void Traverse(MDNode* n, MDNode* parent, int dim, std::string& prefix);

public:
    MDNode* m_head;
    uint32_t m_basis;
    PreAllocator<MDDesc> *desc_allocator;
};


#endif /* end of include guard: MDLIST_H */
