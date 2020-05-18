#DBT-Kimball-Dimensional Test Suite

## Basic Functional Requirements 
Things this repo needs to hit:
Dimensions:
- support of incremental inserts
- support of determenistic full refreshes 
- support of type 0,1,2 slowly changing dimensions
- support of type 4 scd vi array or object column (instead of mini-dimension)
- support of type "10" via array or object column (instead of mini-dimension)
- support of late-arriving data
- support of both mutable source data and immutable event data lakes 
   (e.g. copy of a live changing `USER` table and copy of `USER` table bin logs
- _stretch goal_: support of type 7 slowly changing dimensions
![type 7 SCD](http://www.kimballgroup.com/wp-content/uploads/2013/02/type-711.png)
- _stretch goal_: support of schema / SCD type changes in incremental

#### Type 10 SCD
Not part of the Kimball spec, **type 10** is a modern hybrid of type 4 and 6. 
**Type 4** creates a mini-dimension table:
![type 4 SCD](http://www.kimballgroup.com/wp-content/uploads/2013/02/type-441.png)
**Type 6** exposes both the type 1 & 2 values for an attribute: 
![type 6 SCD](http://www.kimballgroup.com/wp-content/uploads/2013/02/type-621.png)

**type 10** exposes both the type 2 value and a mini-dimension column using semi-structured data.
This mini-dimension column is updated as type 1:
![type 10 SCD](./.readme_assets/type_10_scd.png)

This hybrid SCD enables "alternate reality" lookups without any joins, which is very useful
for things like unified user models. 

Facts:
- Support of incremental inserts
- support of determenistic full refreshes
- _stretch goal_: support of schema / SCD type changes in incremental

Models will need to supply:
- a durable natural key (DNK) column (single or compound).
- a change delta capture (CDC) column to indicate the transform window.\*
\* _Note_: often we use the `current_date` as the `record_captured_at` value in batch processes; this is fine, but the "current date" when the record lands needs to be persisted in the source data sets to support a fully deterministic full refresh. 

Models can optionally supply:
- `type_0`,`type_1`,`type_4`
- `type_10` 
- a "beginning of time" timestamp. When not supplied defaults to January 1, 1970.
- a "lookback window" for CDC columns to aid performance by limiting how late records can be. A lookback window of 0 indicates no support for late arriving records. 


## Out of Scope
- hard deletes (these need to be handled by upstream modeling / EL processes).


## Test Data
This test suite covers 3 data sources and 4 final dimensional models. Each are described in great detail to help make sure the model data accurately represents the full range of test cases.

### Sources

***`_total_replay` datasets***: each dataset has 2 collections of data - records divided up into multiple chronological loads, anda `_total_replay` set that represents a lake of immutable events. a `capture_date` value is added to the total replay set to indicate replay order. Dimensional models should produce the same result from either the `_day_x` source tables applied in order _or_ the `_total_replay` source table applied all at once. 

#### Web\_Event

| **Mutable:**                           | False           |
| **Change Data Capture (CDC) Column:**  | collector\_date |
| **Durable Natural Key (DNK) Column:**  | event\_id       |

This is a simplified version of a web tracker like Google Analytics or Snowplow. Events create new rows. 
No late arriving data is expected. 

#### User

| **Mutable:**                           | True        |
| **Change Data Capture (CDC) Column:**  | batched\_at |
| **Durable Natural Key (DNK) Column:**  | user\_id    |

The `User` table is created by an EL process that consolidates (matches) the live production source `User` table. No hard deletes exist in source. `batched_at` may be late arriving (ie batches do not need to arrive in order).

**Notable Outliers**: 
- user 99901 (Egor Francioli) had a record stuck in the collector on day 1, was late updated on day 3.

#### Order

| **Mutable:**                           | True            |
| **Change Data Capture (CDC) Column:**  | collector\_date |
| **Durable Natural Key (DNK) Column:**  | order\_id       |


The `Order` table is created by an EL process that consolidates (matches) the live production `Order` table. 
No hard deletes exist in source. `collector_date` may be late arriving (ie the collector can not arrive out of order).


#### Order Item

| **Mutable:**                           | False                       |
| **Change Data Capture (CDC) Column:**  | collector\_date             |
| **Durable Natural Key (DNK) Column:**  | collector\_date + order\_id |

The `Order Item` table is created by an EL process that creates a new immutable row every time a change is detected. 
Each order item record is an immutable event representing a change (either a new record or updated record).
No hard deletes exist in source. `collector_date` may be late arriving (ie the collector can be delayed and ship out-of-order. 

### Dimensions

#### DIM\_USER
| **Type 2 Columns**: | first\_name, last\_name, email\_address, phone\_number, is\_preferred\_user, age\_bracket, birthday |
| **Type 1 Columns**: | account\_created\_at                                                                                |
| **Type 10 Columns**:| all\_email\_addresses, all\_phone\_numbers, all\_web\_event\_visitor\_ids                           |


#### DIM\_PRODUCT
| **Type 2 Columns**: | first\_name, last\_name, email\_address, phone\_number, is\_preferred\_user, age\_bracket, birthday |
| **Type 1 Columns**: | account\_created\_at                                                                                |
| **Type 10 Columns**:| all\_email\_addresses, all\_phone\_numbers, all\_web\_event\_visitor\_ids                           |

## Facts

### FACT\_SALE


### FACT\_RETURN
