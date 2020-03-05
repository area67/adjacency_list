#pragma once
#include <stdint.h>
#include <stdlib.h>

enum OpStatus
{
    ACTIVE = 0,
    COMMITTED,
    ABORTED,
};

enum ReturnCode
{
    OK = 0,
    SKIP,
    FAIL,
    RETRY
};

enum OpType
{
    FIND = 0,
    INSERT,
    DELETE,
    INSERT_EDGE,
    DELETE_EDGE
};

struct Operator
{
    uint8_t type;
    uint32_t key;
    uint32_t edge_key;
};

struct Desc
{
    static size_t SizeOf(uint8_t size)
    {
        return sizeof(uint8_t) + sizeof(uint8_t) + sizeof(Operator) * size + sizeof(bool) * size;
    }

    // Status of the transaction: values in [0, size] means live txn, values -1 means aborted, value -2 means committed.
    volatile uint8_t status;
    uint8_t size;
    Operator ops[];
    bool pending[];
};

struct NodeDesc
{
    NodeDesc(Desc* _desc, uint8_t _opid)
        : desc(_desc), opid(_opid){}

    Desc* desc;
    uint8_t opid;
    bool override_as_find = false;
    bool override_as_delete = false;
};