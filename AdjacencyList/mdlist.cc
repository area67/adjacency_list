//------------------------------------------------------------------------------
// 
//     
//
//------------------------------------------------------------------------------

#include <cstdio>
#include <cstring>
#include <immintrin.h>
#include "mdlist.h"

//------------------------------------------------------------------------------
/*template<int D>
inline void MDList::KeyToCoord(uint32_t key, uint8_t coord[])
{
    const static uint32_t basis[32] = {0xffffffff, 0x10000, 0x800, 0x100, 0x80, 0x40, 0x20, 0x10, 0xC, 0xA, 0x8, 0x7, 0x6, 0x5, 0x5, 0x4,
                                        0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x3,0x2};

    uint32_t quotient = key;

    for (int i = D - 1; i >= 0 ; --i) 
    {
        coord[i] = quotient % basis[D - 1];
        quotient /= basis[D - 1];
    }
}*/


template<>
inline void MDList::KeyToCoord<16>(uint32_t key, uint8_t coord[])
{
    static const uint32_t MASK[16] = {0x3u << 30, 0x3u << 28, 0x3u << 26, 0x3u << 24, 0x3u << 22, 0x3u << 20, 0x3u << 18, 0x3u << 16,
                                      0x3u << 14, 0x3u << 12, 0x3u << 10, 0x3u << 8, 0x3u << 6, 0x3u << 4, 0x3u << 2, 0x3u};

    for (uint32_t i = 0; i < 16 ; ++i) 
    {
        coord[i] = ((key & MASK[i]) >> (30 - (i << 1)));
    }
}

//------------------------------------------------------------------------------
MDList::MDList(uint32_t key_range, MDNode *head, PreAllocator<MDDesc> *& d_allocator)
    : m_head(head)
    , m_basis(3 + std::ceil(std::pow((float)key_range, 1.0 / (float)DIMENSION)))
{
    //memset(m_head, 0, sizeof(MDNode));
    desc_allocator = d_allocator;
}

MDList::~MDList()
{
}


//------------------------------------------------------------------------------
bool MDList::Insert(MDNode*& new_node, MDNode*& pred, MDNode*& curr, uint32_t& dim, uint32_t& pred_dim)
{
    MDNode* pred_child = pred->m_child[pred_dim];

    //We are not updating existing node, remove this line to allow update
    if(dim == DIMENSION && !IS_DELINV(pred_child))
    {
        return false;; 
    }

    //There are three possible inserting positions:
    //1. on link edge from pred to curr node
    //   [repalce pointer in pred with new_node and make curr the child of new_node] 
    //
    //2. before curr node
    //   [replace pointer in pred with new_node and copy all children up to dim to new_node] 
    //
    //3. after pred node
    //   [curr node must be NULL, no need to pend task]
    //
    //4. override the current node
    //   [insert new_node and disconnet curr node]

    //Atomically update pred node's child pointer
    //if cas fails, it means another node has succesfully inserted inbetween pred and curr
    MDNode* expected = curr;
    if(IS_DELINV(pred_child))
    {
        expected = SET_DELINV(curr); 
        //if child adoption is need, we take the chance to do physical deletion
        //Otherwise, we CANNOT force full scale adoption
        if(dim == DIMENSION - 1)
        {
            dim = DIMENSION;
        }
    }

    if(pred_child == expected)
    {
        MDDesc* desc = FillNewNode(new_node, pred, expected, dim, pred_dim);

        pred_child = __sync_val_compare_and_swap(&pred->m_child[pred_dim], expected, new_node);

        if(pred_child == expected)
        {
            //The insertion succeeds, complete remaining pending task for this node if any
            if(desc) 
            {
                //Case 2 inserting new_node at the same dimesion as pred node
                //and push the curr node down one dimension
                //We need to help curr node finish the pending task 
                //because the children adoption in new_node might need the adoption from curr node
                MDDesc* pending = curr ? curr->m_pending : NULL;
                if(pending)
                {
                    FinishInserting(curr, pending);
                }

                FinishInserting(new_node, desc);
            }

            return true;
        }
    }

    //If the code reaches here it means the CAS failed
    //Three reasons why CAS may fail:
    //1. the child slot has been marked as invalid by parents
    //2. another thread inserted a child into the slot
    //3. another thread deleted the child
    if(IS_ADPINV(pred_child))
    {
        pred = NULL;
        curr = m_head;
        dim = 0;
        pred_dim = 0;
    }
    else if(CLR_INVALID(pred_child) != curr)
    {
        curr = pred;
        dim = pred_dim;
    }
    else
    {
        //Do nothing, no need to find new inserting poistion
        //retry insertion at the same location
    }

    //If pending taks is allocate free it now
    //it might not be needed in the next try
    //No need to restore other fields as they will be initilized in the next iteration 
    if(new_node->m_pending)
    {
        new_node->m_pending = NULL;
    }

    return false;
}

inline void MDList::LocatePred(uint8_t coord[], MDNode*& pred, MDNode*& curr, uint32_t& dim, uint32_t& pred_dim)
{
    //Locate the proper position to insert
    //traverse list from low dim to high dim
    while(dim < DIMENSION)
    {
        //Loacate predecessor and successor
        while(curr && coord[dim] > curr->m_coord[dim])
        {
            pred_dim = dim;
            pred = curr;

            MDDesc* pending = curr->m_pending;
            if(pending && dim >= pending->pred_dim && dim <= pending->dim)
            {
                FinishInserting(curr, pending);

            }
            curr = CLR_INVALID(curr->m_child[dim]);
        }

        //no successor has greater coord at this dimension
        //the position after pred is the insertion position
        if(curr == NULL || coord[dim] < curr->m_coord[dim]) 
        {
            //done searching
            break;
        }
        //continue to search in the next dimension 
        //if coord[dim] of new_node overlaps with that of curr node
        else
        {
            //dim only increases if two coords are exactly the same
            ++dim;
        }
    }
}

inline MDDesc* MDList::FillNewNode(MDNode* new_node, MDNode*& pred, MDNode*& curr, uint32_t& dim, uint32_t& pred_dim)
{
    MDDesc* desc = NULL;
    if(pred_dim != dim)
    {
        //descriptor to instruct other insertion task to help migrate the children
        desc = (MDDesc*)desc_allocator->get_new();
        desc->curr = CLR_DELINV(curr);
        desc->pred_dim = pred_dim;
        desc->dim = dim;
    }

    //Fill values for new_node, m_child is set to 1 for all children before pred_dim
    //pred_dim is the dimension where new_node is inserted, all dimension before that are invalid for new_node
    for(uint32_t i = 0; i < pred_dim; ++i)
    {
        new_node->m_child[i] = (MDNode*)0x1;
    }
    //be careful with the length of memset, should be DIMENSION - pred_dim NOT (DIMENSION - 1 - pred_dim)
    memset(new_node->m_child + pred_dim, 0, sizeof(MDNode*) * (DIMENSION - pred_dim));
    if(dim < DIMENSION)
    {
        //If curr is marked for deletion or overriden, we donnot link it. 
        //Instead, we adopt ALL of its children
        new_node->m_child[dim] = curr;
    }
    new_node->m_pending = desc;

    return desc;
}

inline void MDList::FinishInserting(MDNode* n, MDDesc* desc)
{
    uint32_t pred_dim = desc->pred_dim;    
    uint32_t dim = desc->dim;    
    MDNode* curr = desc->curr;

    for (uint32_t i = pred_dim; i < dim; ++i) 
    {
        MDNode* child = curr->m_child[i];

        //Children slot of curr_node need to be marked as invalid 
        //before we copy them to new_node
        //while(!IS_ADPINV(child) && !__sync_bool_compare_and_swap(&curr->m_child[i], child, SET_ADPINV(child)))
        //{
            //child = curr->m_child[i];
        //}
        child = __sync_fetch_and_or(&curr->m_child[i], 0x1);
        child = CLR_ADPINV(curr->m_child[i]);
        if(child)
        {
            //Adopt children from curr_node's
            if(n->m_child[i] == NULL)
            {
                __sync_bool_compare_and_swap(&n->m_child[i], NULL, child);
            }
        }
    }

    //Clear the pending task
    if(n->m_pending == desc && __sync_bool_compare_and_swap(&n->m_pending, desc, NULL))
    {
        //TODO:recycle desc
    }
}

bool MDList::Delete(MDNode*& pred, MDNode*& curr, uint32_t pred_dim, uint32_t dim)
{
    if(dim == DIMENSION)
    {
        MDNode* pred_child = pred->m_child[pred_dim];

        if(pred_child == curr)
        {
            pred_child = __sync_val_compare_and_swap(&pred->m_child[pred_dim], curr, SET_DELINV(curr));
        }

        //1. successfully marked node for deletion
        if(pred_child == curr)
        {
            return true;
        }
        //2. Node is marked for deletion by another thread 
        else if(IS_DELINV(pred_child) && CLR_INVALID(pred_child) == curr)
        {
            return false;
        }
    }
    //Node does not exist
    return false;
}

bool MDList::Find(uint32_t key)
{
    //TODO: may be use specilized locatedPred to speedup
    uint8_t coord[DIMENSION];
    KeyToCoord<DIMENSION>(key, coord);
    MDNode* pred = NULL;      //pred node
    MDNode* curr = m_head;    //curr node
    uint32_t dim = 0;       //the dimension of curr node
    uint32_t pred_dim = 0;  //the dimesnion of pred node

    LocatePred(coord, pred, curr, dim, pred_dim);

    return dim == DIMENSION;
}
