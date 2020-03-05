#include <atomic>
#include <limits.h>
#include <iostream>
#include <thread>
#include <cstdlib>
#include <cstdio>
#include <new>
#include "AdjacencyList.h"

#define SET_MARK(_p)    ((Node *)(((uintptr_t)(_p)) | 1))
#define CLR_MARK(_p)    ((Node *)(((uintptr_t)(_p)) & ~1))
#define CLR_MARKD(_p)    ((NodeDesc *)(((uintptr_t)(_p)) & ~1))
#define IS_MARKED(_p)     (((uintptr_t)(_p)) & 1)

__thread AdjacencyList::HelpStack helpStack;

AdjacencyList::AdjacencyList(int num_threads, int _transize, int ops)
	: head(new Node(0, NULL, NULL, NULL))
    , tail(new Node(0xffffffff, NULL, NULL, NULL))
    , thread_count(num_threads)
    , transaction_size(_transize)
    {
        node_allocator = new PreAllocator<Node>(num_threads, sizeof(Node), ops);
        desc_allocator = new PreAllocator<Desc>(num_threads, Desc::SizeOf(_transize), ops);
        ndesc_allocator = new PreAllocator<NodeDesc>(num_threads, sizeof(NodeDesc), ops);
        mdlist_allocator = new PreAllocator<MDList>(num_threads, sizeof(MDList), ops);
        mdnode_allocator = new PreAllocator<MDNode>(num_threads, sizeof(MDNode), ops);
        mddesc_allocator = new PreAllocator<MDDesc>(num_threads, sizeof(MDDesc), ops);
        head->next = tail;
    }

Desc* AdjacencyList::AllocateDesc(uint8_t size)
{
    Desc* desc = desc_allocator->get_new();
    desc->size = size;
    desc->status = ACTIVE;

    for (int i = 0; i < size; i++)
    {
        desc->pending[i] = true;
    }
    
    return desc;
}

void AdjacencyList::Init()
{
    node_allocator->init();
    desc_allocator->init();
    ndesc_allocator->init();
    mdlist_allocator->init();
    mdnode_allocator->init();
    mddesc_allocator->init();
}

bool AdjacencyList::ExecuteOps(Desc* desc)
{
    helpStack.Init();

    HelpOps(desc, 0);

    bool ret = desc->status != ABORTED;

    return ret;
}

inline void AdjacencyList::MarkForDeletion(const std::vector<Node*>& nodes, const std::vector<Node*>& preds, const std::vector<MDNode*>& md_nodes, 
    const std::vector<MDNode*>& md_preds, const std::vector<Node*>& parents, std::vector<uint32_t>& dims, std::vector<uint32_t>& predDims, Desc* desc)
{ 
    // Mark nodes for logical deletion
    for(uint32_t i = 0; i < nodes.size(); ++i)
    {
        Node* n = nodes[i];
        if(n != NULL)
        {
            NodeDesc* node_desc = n->node_desc;

            if(node_desc->desc == desc)
            {
                //Mark node descriptor
                if(__sync_bool_compare_and_swap(&n->node_desc, node_desc, SET_MARK(node_desc)))
                {
                    Node* pred = preds[i];
                    Node* succ = CLR_MARK(__sync_fetch_and_or(&n->next, 0x1));
                    __sync_bool_compare_and_swap(&pred->next, n, succ); //Physical Removal
                }
            }
        }
    }

    //Mark MDList nodes for logical deletion
    for(uint32_t i = 0; i < md_nodes.size(); ++i)
    {
        MDNode* node = md_nodes[i];
        MDNode* pred_node = md_preds[i];
        uint32_t dim = dims[i];
        uint32_t pred_dim = predDims[i];

        if (node != NULL)
        {
            NodeDesc* node_desc = node->node_desc;

            if(node_desc->desc == desc)
            {
                //Mark node descriptor
                if(__sync_bool_compare_and_swap(&node->node_desc, node_desc, SET_MARK(node_desc)))
                {
                    Node* parent = parents[i];
                    parent->m_list->Delete(pred_node, node, dim, pred_dim); //Mark pointer
                }
            }
        }
    }
}

inline void AdjacencyList::HelpOps(Desc* desc, uint8_t opid)
{
    if((int)desc->status != ACTIVE)
    {
        return;
    }

    //Cyclic dependcy check
    if(helpStack.Contain(desc))
    {
        __sync_bool_compare_and_swap(&desc->status, ACTIVE, ABORTED);
        return;
    }

    ReturnCode ret = OK;

    //Vertex Nodes
    std::vector<Node*> delNodes;
    std::vector<Node*> delPredNodes;
    std::vector<Node*> insNodes;
    std::vector<Node*> insPredNodes;

    //Edge Nodes
    std::vector<MDNode*> md_delNodes;
    std::vector<MDNode*> md_delPredNodes;
    std::vector<Node*> md_delParentNodes;
    std::vector<uint32_t> md_delDims;
    std::vector<uint32_t> md_delPredDims;

    //Edge Nodes
    std::vector<MDNode*> md_insNodes;
    std::vector<MDNode*> md_insPredNodes;
    std::vector<Node*> md_insParentNodes;
    std::vector<uint32_t> md_insDims;
    std::vector<uint32_t> md_insPredDims;

    helpStack.Push(desc);

    while(desc->status == ACTIVE && ret != FAIL && opid < desc->size)
    {
        const Operator& op = desc->ops[opid];

        if(op.type == INSERT)
        {
            Node* inserted;
            Node* pred;
            ret = InsertVertex(op.key, desc, opid, inserted, pred);

            insNodes.push_back(inserted);
            insPredNodes.push_back(pred);
        }
        else if(op.type == DELETE)
        {
            Node* deleted;
            Node* pred;
            ret = DeleteVertex(op.key, desc, opid, deleted, pred);        

            delNodes.push_back(deleted);
            delPredNodes.push_back(pred);
        }
        else if (op.type == INSERT_EDGE)
        {
            MDNode* inserted;
            MDNode* md_pred;
            Node* parent;
            uint32_t dim = 0;
            uint32_t pred_dim = 0;

            ret = InsertEdge(op.key, op.edge_key, desc, opid, inserted, md_pred, parent, dim, pred_dim);

            md_insNodes.push_back(inserted);
            md_insPredNodes.push_back(md_pred);
            md_insParentNodes.push_back(parent);
            md_insDims.push_back(dim);
            md_insPredDims.push_back(pred_dim);  

        }
        else if (op.type == DELETE_EDGE)
        {
            MDNode* deleted;
            MDNode* md_pred;
            Node* parent;
            uint32_t dim = 0;
            uint32_t pred_dim = 0;

            ret = DeleteEdge(op.key, op.edge_key, desc, opid, deleted, md_pred, parent, dim, pred_dim);

            md_delNodes.push_back(deleted);
            md_delPredNodes.push_back(md_pred);
            md_delParentNodes.push_back(parent);
            md_delDims.push_back(dim);
            md_delPredDims.push_back(pred_dim);  
        }
        else
        {
            ret = Find(op.key, desc, opid);
        }

        opid++;
    }

    helpStack.Pop();

    if(ret != FAIL)
    {
        if(__sync_bool_compare_and_swap(&desc->status, ACTIVE, COMMITTED))
        {
            MarkForDeletion(delNodes, delPredNodes, md_delNodes, md_delPredNodes, md_delParentNodes, md_delDims, md_delPredDims, desc);
        }
    }
    else
    {
        if(__sync_bool_compare_and_swap(&desc->status, ACTIVE, ABORTED))
        {
            MarkForDeletion(insNodes, insPredNodes, md_insNodes, md_insPredNodes, md_insParentNodes, md_insDims, md_insPredDims, desc);
        }     
    }
}

inline bool AdjacencyList::IsSameOperation(NodeDesc* nodeDesc1, NodeDesc* nodeDesc2)
{
    return nodeDesc1->desc == nodeDesc2->desc && nodeDesc1->opid == nodeDesc2->opid;
}

inline bool AdjacencyList::IsNodeExist(Node* node, uint32_t key)
{
    return node != NULL && node->key == key;
}

//Returns True if the node is physically in the list, but may be logically deleted or part of a pending transaction
inline bool AdjacencyList::IsNodeExist(MDNode* node, uint32_t key)
{
    return node != NULL && node->m_key == key;
}

inline void AdjacencyList::FinishPendingTxn(NodeDesc* nodeDesc, Desc* desc)
{
    if(nodeDesc->desc == desc)
    {
        return;
    }

    //Check for incomplete DeleteVertex operation
    if (nodeDesc->desc->ops[nodeDesc->opid].type == DELETE && nodeDesc->desc->pending[nodeDesc->opid])
    {
        HelpOps(nodeDesc->desc, nodeDesc->opid);
        return;
    }

    HelpOps(nodeDesc->desc, nodeDesc->opid + 1);
}

inline bool AdjacencyList::IsNodeActive(NodeDesc* nodeDesc)
{
    return nodeDesc->desc->status == COMMITTED;
}

//Returns True if the node logically exists
inline bool AdjacencyList::IsKeyExist(NodeDesc* nodeDesc)
{
    bool isNodeActive = IsNodeActive(nodeDesc);
    uint8_t opType = nodeDesc->desc->ops[nodeDesc->opid].type;

    return  (opType == FIND || nodeDesc->override_as_find) || (isNodeActive && (opType == INSERT || opType == INSERT_EDGE)) || (!isNodeActive && (opType == DELETE || opType == DELETE_EDGE || nodeDesc->override_as_delete));
}

inline ReturnCode AdjacencyList::Find(uint32_t key, Desc* desc, uint8_t opid)
{
	Node *pred = nullptr, *current = head;

	NodeDesc *n_desc = NULL;

	while (true)
	{
		LocatePred(pred, current, key);
		
		if(IsNodeExist(current, key))
        {
            NodeDesc* current_desc = current->node_desc;

            if(IS_MARKED(current_desc))
            {
                if(!IS_MARKED(current->next))
                {
                    (__sync_fetch_and_or(&current->next, 0x1));
                }
                current = head;
                continue;
            }

            FinishPendingTxn(current_desc, desc);

            if(n_desc == NULL) 
            {  
                n_desc = new(ndesc_allocator->get_new()) NodeDesc(desc, opid);
            }

            if(IsSameOperation(current_desc, n_desc))
            {
                return SKIP;
            }

            if(IsKeyExist(current_desc))
            {
                if(desc->status != ACTIVE)
                {
                    return FAIL;
                }

                //Update desc 
                if(__sync_bool_compare_and_swap(&current->node_desc, current_desc, n_desc))
                {
                    return OK; 
                }
            }
            else
            {
                return FAIL;
            }
        }
        else 
        {
            return FAIL;
        }
	}
}

inline ReturnCode AdjacencyList::InsertVertex(uint32_t vertex, Desc* desc, uint8_t opid, Node*& inserted, Node*& pred)
{
	inserted = NULL;
    Node *new_node = NULL;
    Node *current = head;

    NodeDesc *n_desc = new(ndesc_allocator->get_new()) NodeDesc(desc, opid);

    while(true)
    {
        LocatePred(pred, current, vertex);

        //Check if node is physically in the list
        if(!IsNodeExist(current, vertex))
        {
            if(desc->status != ACTIVE)
            {
                return FAIL;
            }

            if(new_node == NULL)
            {
                //Allocate new vertex node
                new_node = new(node_allocator->get_new()) Node(vertex, NULL, n_desc, NULL);

                //Allocate mdlist, along with a sentinel head node
                MDNode *mdlist_head = mdnode_allocator->get_new();
                mdlist_head->node_desc = new(ndesc_allocator->get_new()) NodeDesc(desc, opid);

                new_node->m_list = new(mdlist_allocator->get_new()) MDList(key_range, mdlist_head, mddesc_allocator);
            }
            new_node->next = current;

            //Node is not physically in the list, perform physical insertion
            if(__sync_bool_compare_and_swap(&pred->next, current, new_node))
            {
                inserted = new_node;
                return OK;
            }

            current = IS_MARKED(pred->next) ? head : pred;
        }
        else //If the node is physically in the list, it may be possible to simply update the descriptor
        {
            NodeDesc* current_desc = current->node_desc;

            //Check if node descriptor is marked for deletion
            //If it has, we cannot update the descriptor and must perform physical removal
            if(IS_MARKED(current_desc))
            {
                //DO_DELETE
                if(!IS_MARKED(current->next))
                {
                    (__sync_fetch_and_or(&current->next, 0x1));
                }
                current = head;
                continue;
            }

            FinishPendingTxn(current_desc, desc);

            if(IsSameOperation(current_desc, n_desc))
            {
                return SKIP;
            }

            //Check is node is logically in the list
            if(!IsKeyExist(current_desc))
            {
                //Check if our transaction has been aborted by another thread
                if(desc->status != ACTIVE)
                {
                    return FAIL;
                }

                //If the node is not logically in the list, and the descriptor has not been marked yet, we can try to update the descriptor
                //Doing so completes our insert
                if(__sync_bool_compare_and_swap(&current->node_desc, current_desc, n_desc))
                {
                    inserted = current;
                    return OK; 
                }
        
            }
            else
            {
                return FAIL;
            }
        }
    }
}

inline ReturnCode AdjacencyList::DeleteVertex(uint32_t vertex, Desc* desc, uint8_t opid, Node*& deleted, Node*& pred)
{
	deleted = NULL;
    Node *current = head;

    NodeDesc* node_desc = new(ndesc_allocator->get_new()) NodeDesc(desc, opid);

    while(true)
    {
        LocatePred(pred, current, vertex);

        if(IsNodeExist(current, vertex))
        {
            NodeDesc* current_desc = current->node_desc;

            if(IS_MARKED(current_desc))
            {
                return FAIL;
            }

            FinishPendingTxn(current_desc, desc);

            //DeleteVertex is the only operation that is not complete when the node descriptor is placed in the node
            //It is only complete when all edge nodes have also been updated, thus we must check for that case here
            if(IsSameOperation(current_desc, node_desc))
            {
                //Check if deleteVertex operation is ongoing
                if (desc->pending[opid] == false)
                {
                    FinishDeleteVertex(current->m_list, current->m_list->m_head, 0, desc, node_desc, DIMENSION);

                    //Only allow the thread that marks the operation complete to perform physical updates
                    if (__sync_bool_compare_and_swap(&desc->pending[opid], true, false))
                    {
                        deleted = current;
                        return OK;
                    }
                }
                return SKIP;
            }

            if(IsKeyExist(current_desc))
            {
                if(desc->status != ACTIVE)
                {
                    return FAIL;
                }

                if(__sync_bool_compare_and_swap(&current->node_desc, current_desc, node_desc))
                {
                    FinishDeleteVertex(current->m_list, current->m_list->m_head, 0, desc, node_desc, DIMENSION);

                    //Only allow the thread that marks the operation complete to perform physical updates
                    if (__sync_bool_compare_and_swap(&desc->pending[opid], true, false))
                    {
                        deleted = current;
                        return OK;
                    }
                }
            }
            else
            {
                return FAIL;
            }  
        }
        else 
        {
            return FAIL;      
        }
    }
}

inline void AdjacencyList::FinishDeleteVertex(MDList* m_list, MDNode* n, int dim, Desc *desc, NodeDesc *node_desc, int DIMENSION)
{
    //Update Desc
    while (true)
    {
        NodeDesc* current_desc = n->node_desc;

        if (current_desc == NULL || IS_MARKED(current_desc))
        {
            break;
        }

        FinishPendingTxn(current_desc, desc);

        if(desc->status != ACTIVE)
        {
            return;
        }

        //Move on to the next children if we either succeed a CAS to update the descriptor or we see that a different thread has already done so
        if(IsSameOperation(current_desc, node_desc) || __sync_bool_compare_and_swap(&n->node_desc, current_desc, node_desc))
        {
            MDDesc* pending = n->m_pending;
            //If a pending child adoption is occuring, make sure it completes so that no nodes are missed in traversal
            //A new child adoption cannot occur at this node, as our mdlist only creates adoption descriptors in new nodes during insertion
            if (pending)
            {
                m_list->FinishInserting(n, pending);
            }

            for (int i = DIMENSION - 1; i >= dim; --i) 
            {
                MDNode *child = CLR_INVALID(n->m_child[i]);

                if(child != NULL)
                {
                    FinishDeleteVertex(m_list, child, i, desc, node_desc, DIMENSION);
                }
            }

            break;
        }
    }
}

//A helping function for InsertEdge and DeleteEdge
//Verifies that a vertex is logically in the list
inline bool AdjacencyList::FindVertex(Node*& curr, NodeDesc*& n_desc, Desc *desc, uint32_t key)
{
    curr = head;
    Node *pred = NULL;

    while(true)
    {
        LocatePred(pred, curr, key);

        if(IsNodeExist(curr, key))
        {
            NodeDesc* current_desc = curr->node_desc;

            if(IS_MARKED(current_desc))
            {
                //Node descriptor is marked for deletion
                return false;
            }

            FinishPendingTxn(current_desc, desc);

            if(IsKeyExist(current_desc))
            {
                //Vertex is logically within the list
                return true;
            }
            else
            {
                //Vertex is physically in the list, but logically deleted
                return false;
            }
        }
        else 
        {
            //Vertex is not physically in the list
            return false;
        }
    }
}

inline ReturnCode AdjacencyList::InsertEdge(uint32_t vertex, uint32_t edge, Desc* desc, uint8_t opid, MDNode*& inserted, MDNode*& md_pred, Node*& current, uint32_t& dim, uint32_t& pred_dim)
{
    inserted = NULL;
    md_pred = NULL;

    NodeDesc* n_desc = new(ndesc_allocator->get_new()) NodeDesc(desc, opid);
    MDNode *new_node = mdnode_allocator->get_new();
    MDList* mdlist;

    new_node->m_key = edge;
    new_node->node_desc = n_desc;

    MDNode* md_current;

    //Try to find the vertex to which the current key is adjacenct
    if (FindVertex(current, n_desc, desc, vertex))
    {
        mdlist = current->m_list;
        md_current = mdlist->m_head;
        mdlist->KeyToCoord<DIMENSION>(edge, new_node->m_coord);
        while(true)
        {
            mdlist->LocatePred(new_node->m_coord, md_pred, md_current, dim, pred_dim);

            //Check if the node is physically not within the list, or that it is there, but marked for deletion
            //If it is marked for deletion, the mdlist will physically remove it during the call to mdlist->Insert
            if(!IsNodeExist(md_current, edge) || IS_DELINV(md_pred->m_child[pred_dim]))
            {
                //Check if our transaction has been aborted by another thread
                if(desc->status != ACTIVE)
                {
                    return FAIL;
                }

                //Update pred descriptor before inserting our new node, as this is the only way for a concurrent DeleteVertex to find our operation
                //If we do not update the pred descriptor, the new node may be inserted after DeleteVertex has traversed past this node
                NodeDesc* pred_current_desc = md_pred->node_desc;
                NodeDesc* pred_desc;

                FinishPendingTxn(CLR_MARKD(pred_current_desc), desc);

                bool same_op = IsSameOperation(pred_current_desc, n_desc);

                //If the pred_current_desc isn't the same op as ours, we need to prepare to update it with a special node_desc
                if(!same_op)
                {
                    bool exists = IsKeyExist(CLR_MARKD(pred_current_desc));

                    //Create a special descriptor that maintains the nodes logical status
                    pred_desc = new(ndesc_allocator->get_new()) NodeDesc(desc, opid);

                    //Node exists, force this operation is display as "find"
                    if (exists)
                        pred_desc->override_as_find = true;
                    else //Node doesn't exist, if we treated the operation as "find" it would add the node back into the list
                        pred_desc->override_as_delete = true;
                }

                //Update the pred node's descriptor, which provides the necessary synchronization to prevent a conflicting deleteVertex from breaking isolation
                //There are 3 cases that InsertEdge and DeleteVertex can interleave, all cases begin after InsertEdge suceeds in updating md_pred's node_desc. 
                //
                //Case 1: md_pred is unmarked, OR md_pred is marked but not physically removed during the InsertEdge operation
                //      DeleteVertex will find the descriptor in md_pred and help complete the transaction before proceeding
                //Case 2: md_pred is physically removed during the InsertEdge 
                //      DeleteVertex will find an adoption descriptor in md_pred's predecessor. This descriptor will move all children of md_pred to that node.
                //      If InsertEdge sucessfully added it's new node to md_pred, the DeleteVertex will find it after the adoption process.
                //      If InsertEdge is too slow to add it's new node, its CAS will fail during the insert process, and it will re-traverse 
                if(same_op || __sync_bool_compare_and_swap(&md_pred->node_desc, pred_current_desc, pred_desc))
                {
                    //Do Insert
                    bool result = mdlist->Insert(new_node, md_pred, md_current, dim, pred_dim);

                    if(result)
                    {
                        inserted = new_node;
                        return OK;
                    }
                }
                //If we don't suceed, retry traversal from wherever md_current is currently pointing
            }
            else 
            {
                NodeDesc* current_desc = md_current->node_desc;

                //Node needs to be deleted
                if(IS_MARKED(current_desc))
                {
                    //Mark the MDList node for deletion and retry
                    //The physical deletion will occur during a call to mdlist->Insert (mdlist only performs physical deletion during insert operations)
                    if(!IS_DELINV(md_pred->m_child[pred_dim]))
                    {
                        __sync_bool_compare_and_swap(&md_pred->m_child[pred_dim], md_current, SET_DELINV(md_current));
                    }
                    md_current = mdlist->m_head;
                    dim = 0;
                    pred_dim = 0;
                    continue;
                }

                FinishPendingTxn(current_desc, desc);

                if(IsSameOperation(current_desc, n_desc))
                {
                    return SKIP;
                }

                //Node exists but is logically deleted, update descriptor to complete insert
                if(!IsKeyExist(current_desc))
                {
                    if(desc->status != ACTIVE)
                    {
                        return FAIL;
                    }

                    if(__sync_bool_compare_and_swap(&md_current->node_desc, current_desc, n_desc))
                    {
                        return OK; 
                    }
            
                }
                else
                {
                    return FAIL;
                }
            }
        }
    }
    else
    {
        return FAIL;
    }
}

inline ReturnCode AdjacencyList::DeleteEdge(uint32_t vertex, uint32_t edge, Desc* desc, uint8_t opid, MDNode*& deleted, MDNode*& md_pred, Node*& current, uint32_t& dim, uint32_t& pred_dim)
{
    deleted = NULL;
    md_pred = NULL;
    current = head;

    NodeDesc *n_desc = new(ndesc_allocator->get_new()) NodeDesc(desc, opid);

    MDList* mdlist;
    MDNode *md_current;
    uint8_t m_coord[DIMENSION];

    //Try to find the vertex to which the current key is adjacenct
    if (FindVertex(current, n_desc, desc, vertex))
    {
        mdlist = current->m_list;
        md_current = mdlist->m_head;
        mdlist->KeyToCoord<DIMENSION>(edge, m_coord);
        while(true)
        {
            mdlist->LocatePred(m_coord, md_pred, md_current, dim, pred_dim);

            if(IsNodeExist(md_current, edge))
            {
                NodeDesc* current_desc = md_current->node_desc;

                if(IS_MARKED(current_desc))
                {
                    return FAIL;
                }

                FinishPendingTxn(current_desc, desc);

                if(IsSameOperation(current_desc, n_desc))
                {
                    return SKIP;
                }

                if(IsKeyExist(current_desc))
                {
                    if(desc->status != ACTIVE)
                    {
                        return FAIL;
                    }

                    if(__sync_bool_compare_and_swap(&md_current->node_desc, current_desc, n_desc))
                    {
                        deleted = md_current;
                        return OK; 
                    }
                }
                else
                {
                    return FAIL;
                }  
            }
            else 
            {
                return FAIL;      
            }
        }
    }
    else
    {
        return FAIL;
    }
}

inline void AdjacencyList::LocatePred(Node*& pred, Node*& current, uint32_t key)
{
    Node* pred_next;

    while(current->key < key)
    {
        pred = current;
        pred_next = CLR_MARK(pred->next);
        current = pred_next;

        while(IS_MARKED(current->next))
        {
            current = CLR_MARK(current->next);
        }

        if(current != pred_next)
        {
            //Failed to remove deleted nodes, start over from pred
            if(!__sync_bool_compare_and_swap(&pred->next, pred_next, current))
            {
                current = head;
            }
        }
    }
}
