#ifndef PRE_ALLOC_H
#define PRE_ALLOC_H

#include <stdint.h>
#include <stdlib.h>
#include <malloc.h>
#include <iostream>

template<typename T>
class PreAllocator
{
public:
    PreAllocator(uint64_t num_threads, uint64_t type_size, uint64_t amount) :
        num_threads(num_threads),
        type_size(type_size),
        amount(amount)
    {
        data = memalign(type_size, num_threads*type_size*amount);
        thread_id_count = 0;

        if (data)
            std::cout << "Allocating room for: " << amount*num_threads << " objects, total size: " << num_threads*type_size*amount <<  "\n";
        else 
        {
            std::cout << "Malloc error in pre_alloc.h, exiting\n";
            exit(EXIT_FAILURE);
        } 
    }

    ~PreAllocator()
    {

    }

    //Each thread must call init before trying to use the Pre-Allocator
    void init()
    {
        id = __sync_fetch_and_add(&thread_id_count, 1);
        base = (uint64_t)data + (id * (type_size * amount));
        index = 0;
    }

    T *get_new()
    {
        if (index > amount)
        {
            std::cout << "Error: out of memory in allocator\n";
            exit(EXIT_FAILURE);
        }

        uint64_t next_item = base + (index * type_size);
        index++;

        return (T *)next_item;
    }

    void free_all()
    {
        return;
    }

    uint64_t thread_id_count;
    uint64_t num_threads;
    uint64_t type_size;
    uint64_t amount;
    void *data;
    static __thread uint64_t id;
    static __thread uint64_t index;
    static __thread uint64_t base;
};

template<typename T>
__thread uint64_t PreAllocator<T>::id;

template<typename T>
__thread uint64_t PreAllocator<T>::index;

template<typename T>
__thread uint64_t PreAllocator<T>::base;

#endif