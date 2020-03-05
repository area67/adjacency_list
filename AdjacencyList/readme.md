# Lock-Free Transactional Adjacency List

## To Compile:
    issue $make

## To Execute:
    issue $./main for parameters

## Parameters:
    <TestSize>: The number of transactions to be executed
    <TransactionSize>: The amount of operation per transaction
    <Threads>: The amount of threads to perform the test with
    <KeyRange>: Range of keys to be randomly inserted into the list
    <InsertVertexRatio>: The ratio of InsertVertex operations, range: [0,1)
    <DeleteVertexRatio>: The ratio of DeleteVertex operations, range: [0,1)
    <InsertEdgeRatio>: The ratio of InsertEdge operations, range: [0,1)
    <DeleteEdgeRatio>: The ratio of DeleteEdge operations, range: [0,1)
    <FindRatio>: The ratio of Find operations, range: [0,1)

## Dependencies
    * Boost
    * pthreads

## More Information
    Refer to publication:
        https://link.springer.com/chapter/10.1007/978-3-030-35225-7_14