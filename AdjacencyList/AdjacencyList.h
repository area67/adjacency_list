#pragma once
#include <atomic>
#include <mutex>
#include <vector>
#include "lftt.h"
#include "pre_alloc.h"
#include "mdlist.h"

class AdjacencyList
{
public:
    static const uint32_t key_range = 1024;
	
	struct Node
	{
		Node(uint32_t _key, Node* _next, NodeDesc* _nodeDesc, MDList* m_list)
            : key(_key), next(_next), node_desc(_nodeDesc), m_list(NULL){}

        uint32_t key;	//Vertex key
		Node *next; 	//Next vertex
        NodeDesc* node_desc;
		MDList *m_list;	//Adjacencies
	};

	struct HelpStack
    {
        void Init()
        {
            index = 0;
        }

        void Push(Desc* desc)
        {
            if (index >= 255)
            {
                printf("index out of range\n");
                std::exit(EXIT_FAILURE);
            }

            helps[index++] = desc;
        }

        void Pop()
        {
            if (index == 0)
            {
                printf("nothing to pop\n");
                std::exit(EXIT_FAILURE);
            }

            index--;
        }

        bool Contain(Desc* desc)
        {
            for(uint8_t i = 0; i < index; i++)
            {
                if(helps[i] == desc)
                {
                    return true;
                }
            }

            return false;
        }

        Desc* helps[256];
        uint8_t index;
    };

    AdjacencyList(int num_threads, int _transize, int ops);

    bool ExecuteOps(Desc* desc);
    void Init();
    Desc* AllocateDesc(uint8_t size);
    void InitLists();

private:
	ReturnCode InsertVertex(uint32_t vertex, Desc* desc, uint8_t opid, Node*& inserted, Node*& pred);
	ReturnCode InsertEdge(uint32_t vertex, uint32_t edge, Desc* desc, uint8_t opid, MDNode*& inserted, MDNode*& md_pred, Node*& current, uint32_t& dim, uint32_t& pred_dim);
	ReturnCode DeleteVertex(uint32_t vertex, Desc* desc, uint8_t opid, Node*& deleted, Node*& pred);
	ReturnCode DeleteEdge(uint32_t vertex, uint32_t edge, Desc* desc, uint8_t opid, MDNode*& deleted, MDNode*& md_pred, Node*& current, uint32_t& dim, uint32_t& pred_dim);
	ReturnCode Find(uint32_t key, Desc* desc, uint8_t opid);

	void HelpOps(Desc* desc, uint8_t opid);
    bool IsSameOperation(NodeDesc* nodeDesc1, NodeDesc* nodeDesc2);
    void FinishPendingTxn(NodeDesc* nodeDesc, Desc* desc);
    void FinishDeleteVertex(MDList* m_list, MDNode* n, int dim, Desc *desc, NodeDesc *nodeDesc, int DIMENSION);
    bool IsNodeExist(Node* node, uint32_t key);
    bool IsNodeExist(MDNode* node, uint32_t key);
    bool IsNodeActive(NodeDesc* nodeDesc);
    bool IsKeyExist(NodeDesc* nodeDesc);
    void LocatePred(Node*& pred, Node*& curr, uint32_t key);
    bool FindVertex(Node*& curr, NodeDesc*& nDesc, Desc *desc, uint32_t key);
    void MarkForDeletion(const std::vector<Node*>& nodes, const std::vector<Node*>& preds, const std::vector<MDNode*>& md_nodes, 
        const std::vector<MDNode*>& md_preds, const std::vector<Node*>& parents, std::vector<uint32_t>& dims, std::vector<uint32_t>& predDims, Desc* desc);

public:
	//Sentinel Nodes
	Node* head;
	Node* tail;

    int thread_count;
    int transaction_size;

    PreAllocator<Node> *node_allocator;
    PreAllocator<Desc> *desc_allocator;
    PreAllocator<NodeDesc> *ndesc_allocator;
    PreAllocator<MDList> *mdlist_allocator;
    PreAllocator<MDNode> *mdnode_allocator;
    PreAllocator<MDDesc> *mddesc_allocator;

};