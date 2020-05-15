#DBT-Kimball-Dimensional Test Suite

## Basic Functional Requirements 
Things this repo needs to hit:
Dimensions:
- support of incremental inserts
- support of determenistic full refreshes 
- support of type 0,1,2 slowly changing dimensions
- support of type 4 via array or object column (instead of mini-dimension)
- support of late-arriving data
- _stretch goal_: support of type 7 slowly changing dimensions

![type 7 SCD](http://www.kimballgroup.com/wp-content/uploads/2013/02/type-711.png)

Facts:
- Support of incremental inserts
- support of determenistic full refreshes

Models will need to supply:
- a durable natural key (single or compound)
- a record\_captured\_at value (or record\_updated\_at value) to indicate the transform window.\*
\* _Note_: often we use the `current_date` as the `record_captured_at` value in batch processes; this is fine, but the "current date" when the record lands needs to be persisted in the source data sets to support a fully deterministic full refresh. 

## Test Data
This test suite covers 4 data sources and 6 final dimensional models. Each are described in great detail to help make sure the model data accurately represents the full range of test cases.

### Sources

#### Web\_Event

#### User

#### Order

### Dimensions

#### DIM\_USER

#### DIM\_WEB\_PAGE

#### DIM\_ORDER\_ITEM

## Facts

### FACT\_SALE

### FACT\_SHIPMENT

### FACT\_WEBSITE\_VISIT


