***************
Fact Strategies
***************

Kimball facts can take on several forms, depending on the nature of the data in question. 

To make life easier, this package uses `fuction overloading <https://en.wikipedia.org/wiki/Function_overloading>`_ to identify and 
execute the correct version of Fact for your use case. 

.. Hint::
   The *fact type* used is determined by the arguments you pass to **config**:
   
   - **Accumulating Fact** will be used if the ``unique_expression`` and ``lookback_window`` args are present.
   - **Complex Fact** will be used if just the ``lookback_window`` arg is present.
   - **Simple Fact** will be used if these are both absent. 


Simple Facts
============

+-------------------------+----+
| **uniquely_keyed**      | No |
+-------------------------+----+
| **mutable**             | No |
+-------------------------+----+
| **late-arriving-fact**  | No |
+-------------------------+----+
| **late-arriving-dim**   | No |
+-------------------------+----+

Simple facts are, as the name suggests, very simple. 

**Key Differentiators of a Simple Fact**

* For every occurance of a simple fact record, a new fact row is created. 
* Records are added to the source data in chronological order; that is, an event occuring on January 1, 2020 will always appear in the source data before an event occurring on January 10, 2020. 
* Simple facts never change - like a web event that is recorded once and never modified.
* Simple facts arrive at the same time or after their associated dims. 
* Because simple facts are so simple, they do not require unique keys - so 3 identical fact records will be recorded as 3 distinct fact instances. 

Complex Facts
==================

+-------------------------+-----+
| **uniquely_keyed**      | No  |
+-------------------------+-----+
| **mutable**             | No  |
+-------------------------+-----+
| **late-arriving-fact**  | Yes |
+-------------------------+-----+
| **late-arriving-dim**   | Yes |
+-------------------------+-----+

Complex facts add the ability to deal with EL and process complexity. 

**Key Differentiators of a Complex Fact**

* For every occurance of a complex fact record, a new fact row is created. 
* Because complex facts allow for late-arriving records, a ``lookback_window`` is required; this window determines how far back DBT should look. 
* The ``lookback_window`` applies to both late-arriving dims and facts. 
* A lookback of ``0`` indicates no lookback (in which case a `Simple Fact <Simple Facts>`_ can likely be used). In this manner, a model with a lookback of ``5`` will correctly handle a fact occurring on January 1st, 2020 if it arrives on or before January 6th, 2020.
  Complex facts support late-arriving dims; this means a fact may have a dim value of ``-1`` (the ``N/A`` dim) until the correct dim arrives. 
* Note that if the late dim arrives after the lookback window it will not be updated. 
* A lookback of ``all`` will instruct DBT to scan the entire fact table for updates on each build; this is very powerful
  but can also become very costly. 
* While a resulting fact record may update over time to accommodate late arriving dims, the fact itself does not change.
* Complex facts use the hashed value of non-dim-key attributes to determine uniqueness, so they do not require an explicit unique key. 

Accumulating Facts
==================

+-------------------------+-----+
| **uniquely_keyed**      | Yes |
+-------------------------+-----+
| **mutable**             | Yes |
+-------------------------+-----+
| **late-arriving-fact**  | Yes |
+-------------------------+-----+
| **late-arriving-dim**   | Yes |
+-------------------------+-----+

Accumulating facts emulate `Kimball Accumulating Snapashots <https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/accumulating-snapshot-fact-table/>`_.
These facts are the most complex of the fact materializations, and allow for fact records to create historical breadcrumbs from 
sequential events. 
 
**Key Differentiators of an Accumulating Fact**

* For every occurance of a newly unique fact record, a new fact row is created. 
* Because accumulating facts allow for late-arriving records *and* "trickling-in" components to current records, a ``lookback_window`` is required; this window determines how far back DBT should look. 
* The ``lookback_window`` applies to both late-arriving dims and facts as well as the "closing" of accumulations.
* Records who's occurance timeline has passed the lookback window become "closed"; these records are now considered immutable and will behave as such.
* A lookback of ``0`` indicates no lookback (in which case a `Simple Fact <Simple Facts>`_ can likely be used). In this manner, a model with a lookback of ``5`` will correctly handle a fact occurring on January 1st, 2020 if it arrives on or before January 6th, 2020.
* Accumulating facts support late-arriving dims; this means a fact may have a dim value of ``-1`` (the ``N/A`` dim) until the correct dim arrives. 
* Note that if the late dim arrives after the lookback window it will not be updated. 
* A lookback of ``all`` will instruct DBT to scan the entire fact table for updates on each build; this is very powerful
  but can also become very costly. This also means the resulting records will never "close".
* Accumulating facts retain their initial values but are mutable in an *additive fashion*; that is, marker values like ``shipped_at`` and ``delivered_at`` can move from ``NULL`` to a valid value 
  during the lookback window.
